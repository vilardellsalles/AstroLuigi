import shutil
import os.path
from tempfile import gettempdir

import luigi

from astroluigi import votable_database as db
from astroluigi import ccdred as ccd


class Reduction(luigi.WrapperTask):
    obsdata = luigi.parameter.Parameter(default=".")
    suffix = luigi.parameter.Parameter(default="imr.fits")
    database = luigi.parameter.Parameter(default="database.xml")

    def requires(self):
        image_list = []
        for path, subdirs, files in os.walk(self.obsdata):
            for image in files:
                image_list += [os.path.join(path, image)]

        yield db.CreateDB(image_list=image_list, database=self.database)

        for image in image_list:
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
        valid_header = [{"keyword": "OBSTYPE", "constant": "Bias"}]
        valid_header += [{"keyword": "INSTRUME"}]
        valid_header += [{"keyword": "FILTER", "constant": "C"}]
        valid_header += [{"keyword": "JD", "type": "int"}]
        valid_header += [{"keyword": "NAXIS1", "type": "int"}]
        valid_header += [{"keyword": "NAXIS2", "type": "int"}]
        valid_header += [{"keyword": "EXPTIME", "constant": 0,
                          "type": "int"}]
        valid_header += [{"keyword": "CAMTEMP", "constant": 3,
                          "type": "float", "operation": "diff"}]

        bias_list = os.path.splitext(os.path.basename(self.image))[0] + ".blst"

        bias = yield db.ImCalib(ref_image=self.image, database=self.database,
                                keywords=valid_header, min_number=5,
                                image_list=bias_list)

        master_bias = yield ccd.ZeroCombine(bias_list=bias.content)

        master_copy = os.path.basename(master_bias.path)
        if not os.path.isfile(master_copy):
            master_bias.copy(master_copy)

        valid_header = [{"keyword": "OBSTYPE", "constant": "Dark"}]
        valid_header += [{"keyword": "INSTRUME"}]
        valid_header += [{"keyword": "FILTER", "constant": "C"}]
        valid_header += [{"keyword": "JD", "type": "int"}]
        valid_header += [{"keyword": "NAXIS1", "type": "int"}]
        valid_header += [{"keyword": "NAXIS2", "type": "int"}]
        valid_header += [{"keyword": "EXPTIME", "operation": "ge"}]
        valid_header += [{"keyword": "CAMTEMP", "constant": 3,
                          "type": "float", "operation": "diff"}]

        dark_list = os.path.splitext(os.path.basename(self.image))[0] + ".dlst"

        dark = yield db.ImCalib(ref_image=self.image, database=self.database,
                                keywords=valid_header, min_number=5,
                                image_list=dark_list)

        master_dark = yield ccd.DarkCombine(dark_list=dark.content,
                                            bias=master_bias.path)

        master_copy = os.path.basename(master_dark.path)
        if not os.path.isfile(master_copy):
            master_dark.copy(master_copy)

        valid_header = [{"keyword": "OBSTYPE", "constant": "Flat"}]
        valid_header += [{"keyword": "INSTRUME"}]
        valid_header += [{"keyword": "FILTER"}]
        valid_header += [{"keyword": "NAXIS1", "type": "int"}]
        valid_header += [{"keyword": "NAXIS2", "type": "int"}]
        valid_header += [{"keyword": "CAMTEMP", "constant": 3,
                          "type": "float", "operation": "diff"}]
        valid_header += [{"keyword": "FOCUSPOS", "constant": 150,
                          "type": "float", "operation": "diff"}]

        flat_list = os.path.splitext(os.path.basename(self.image))[0] + ".flst"

        flat = yield db.ImCalib(ref_image=self.image, database=self.database,
                                keywords=valid_header, min_number=3,
                                image_list=flat_list)

        master_flat = yield ccd.FlatCombine(flat_list=flat.content,
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
