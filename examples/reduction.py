import shutil
import os.path
from tempfile import gettempdir

import luigi
from astropy.io import votable

from astroluigi.votable_database import CreateDB
from astroluigi import ccdred as ccd


class Reduction(luigi.Task):
    obsdata = luigi.parameter.Parameter(default=".")
    suffix = luigi.parameter.Parameter(default="imr.fits")
    database = luigi.parameter.Parameter(default="database.xml")

    def requires(self):
        return CreateDB(location=self.obsdata, database=self.database)

    def run(self):
        database = votable.parse_single_table(self.database).to_table()

        for image in database["file"]:
            if image.endswith(self.suffix):
                new_image = os.path.basename(image)
                yield ImageReduction(image=image, database=self.database,
                                     out_image=new_image)

        # Clean temporary files

        try:
            shutil.rmtree(os.path.join(gettempdir(), "astroluigi"))
        except FileNotFoundError:
            pass


class ImageReduction(luigi.Task):
    image = luigi.parameter.Parameter()
    extension = luigi.parameter.IntParameter(default=0)
    database = luigi.parameter.Parameter(default="database.xml")
    out_image = luigi.parameter.Parameter()

    def output(self):
        return luigi.file.LocalTarget(self.out_image)

    def run(self):
        filter_table = votable.parse_single_table(self.database).to_table()
        ref_image = filter_table[filter_table["file"] == self.image]

        mask = filter_table["obstype"] == "Bias"
        mask &= filter_table["instrume"] == ref_image["instrume"]
        mask &= filter_table["filter"] == "C"
        mask &= filter_table["jd"].astype(int) == int(ref_image["jd"])
        mask &= filter_table["naxis1"].astype(int) == int(ref_image["naxis1"])
        mask &= filter_table["naxis2"].astype(int) == int(ref_image["naxis2"])
        mask &= filter_table["exptime"].astype(int) == 0
        mask &= abs(filter_table["camtemp"] - ref_image["camtemp"]) < 3

        bias_list = filter_table["file"][mask].tolist()

        master_bias = yield ccd.ZeroCombine(bias_list=bias_list)

        master_copy = os.path.basename(master_bias.path)
        if not os.path.isfile(master_copy):
            master_bias.copy(master_copy)

        mask = filter_table["obstype"] == "Dark"
        mask &= filter_table["instrume"] == ref_image["instrume"]
        mask &= filter_table["filter"] == "C"
        mask &= filter_table["jd"].astype(int) == int(ref_image["jd"])
        mask &= filter_table["naxis1"].astype(int) == int(ref_image["naxis1"])
        mask &= filter_table["naxis2"].astype(int) == int(ref_image["naxis2"])
        mask &= abs(filter_table["camtemp"] - ref_image["camtemp"]) < 3

        dark_list = filter_table["file"][mask].tolist()

        master_dark = yield ccd.DarkCombine(dark_list=dark_list,
                                            bias=master_bias.path)

        master_copy = os.path.basename(master_dark.path)
        if not os.path.isfile(master_copy):
            master_dark.copy(master_copy)

        mask = filter_table["obstype"] == "Flat"
        mask &= filter_table["instrume"] == ref_image["instrume"]
        mask &= filter_table["filter"] == ref_image["filter"]
        mask &= filter_table["naxis1"].astype(int) == int(ref_image["naxis1"])
        mask &= filter_table["naxis2"].astype(int) == int(ref_image["naxis2"])
        mask &= abs(filter_table["camtemp"] - ref_image["camtemp"]) < 3
        mask &= abs(filter_table["focuspos"] - ref_image["focuspos"]) < 150

        flat_list = filter_table["file"][mask].tolist()

        master_flat = yield ccd.FlatCombine(flat_list=flat_list,
                                            dark=master_dark.path,
                                            bias=master_bias.path)

        master_copy = os.path.basename(master_flat.path)
        if not os.path.isfile(master_copy):
            master_flat.copy(master_copy)

        bias_corrected = yield ccd.BiasSubtract(self.image,
                                                bias=master_bias.path,
                                                output_file=self.output().path)


if __name__ == "__main__":

    luigi.run(main_task_cls=Reduction)
