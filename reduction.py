import sys
import os.path

import luigi

from astropy.io import fits
from astropy.io import votable
from astropy.table import Table


class Reduction(luigi.WrapperTask):
    obsdata = luigi.parameter.Parameter(default=".")
    extension = luigi.parameter.Parameter(default="imr.fits")
    database = luigi.parameter.Parameter(default="database.xml")

    def requires(self):
        image_list = []
        for path, subdirs, files in os.walk(self.obsdata):
            for image in files:
                image_list += [os.path.join(path, image)]

        yield Store(image_list=image_list, database=self.database)

#        for image in image_list:
#            if image.endswith(self.extension):
#                task_list += [ImageReduction(image=image,
#                                             database=self.database)]
#
#        return task_list


class ImageReduction(luigi.WrapperTask):
    image = luigi.parameter.Parameter()
    database = luigi.parameter.Parameter(default="database.xml")

    def requires(self):
        bias = yield BiasCombine(database=self.database,
                                keywords={"OBSTYPE": "Bias"},
                                out_list="master_bias.fits")


class Store(luigi.Task):
    image_list = luigi.parameter.ListParameter()
    database = luigi.parameter.Parameter()

    def output(self):
        return luigi.file.LocalTarget(self.database)

    def run(self):
        hdu_table = None
        for image in self.image_list:
            try:
                hdulist = fits.open(image)

                if len(hdulist) > 1:
                    errmsg = "Store cannot work with multiple extensions"
                    raise NotImplementedError(errmsg)
 
                for num, hdu in enumerate(hdulist):
                    values = [image, num]
                    values += [value for name, value in hdu.header.items()
                               if name != "COMMENT" and name != "HISTORY"] 
 
                    if hdu_table is None:
                        columns = ["FILENAME", "EXTENSION"]
                        columns += [name for name in hdu.header.keys()
                                    if name != "COMMENT" and name != "COMMENT"]
                        coltypes = [object] *len(columns)
                        hdu_table = Table(names=columns, dtype=coltypes)
 
                    hdu_table.add_row(values)

            except OSError:
                # Not a FITS image
                pass

        out_table = votable.from_table(hdu_table)
        out_table.to_xml(self.output().path)


if __name__ == "__main__":

    luigi.run(main_task_cls=Reduction)
