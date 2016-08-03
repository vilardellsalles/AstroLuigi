import os.path

import luigi
from astropy.io import fits

import database as db
import ccdred as ccd


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

        yield db.ImCalib(ref_image=self.image, database=self.database,
                         keywords=valid_header, image_list=bias_list)

        bias_images = []
        try:
            with open(bias_list, "r") as blst:
                for image in blst:
                    bias_images += [image.strip()]
        except FileNotFoundError:
            pass

        if len(bias_images) < 5:
            print("Returning with:", len(bias_images))
            return

        master_bias = yield ccd.ZeroCombine(bias_list=bias_images)

        with self.output().open("w"):
            pass


if __name__ == "__main__":

    luigi.run(main_task_cls=Reduction)
