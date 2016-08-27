import os.path

import luigi
import numpy as np
from astropy.io import fits
from astropy.io import votable
from astropy.table import Table

from .targets import ASCIITarget

class CreateDB(luigi.Task):
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
                    errmsg = "Storing image with multiple extensions"
                    raise NotImplementedError(errmsg)
 
                for num, hdu in enumerate(hdulist):
                    values = [image, num]
                    values += [value for name, value in hdu.header.items()
                               if name != "COMMENT" and name != "HISTORY"] 
 
                    if hdu_table is None:
                        columns = ["FILENAME", "EXTENSION"]
                        columns += [name for name in hdu.header
                                    if name != "COMMENT" and name != "COMMENT"]

                        coltypes = []
                        for elem in values:
                            if type(elem) == str:
                                elem_len = len(elem)
                                minsize = elem_len if elem_len > 64 else 64
                                coltypes += [np.dtype(str) * minsize]
                            else:
                                coltypes += [np.array(elem).dtype]

                        hdu_table = Table(names=columns, dtype=coltypes)
 
                    hdu_table.add_row(values)

            except OSError:
                # Not a FITS image
                pass

        out_table = votable.from_table(hdu_table)
        out_table.to_xml(self.output().path)


class ImCalib(luigi.Task):
    ref_image = luigi.parameter.Parameter()
    database = luigi.parameter.Parameter()
    keywords = luigi.parameter.ListParameter()
    min_number = luigi.parameter.IntParameter(default=0)
    image_list = luigi.parameter.Parameter()

    def output(self):
        return ASCIITarget(self.image_list)

    def run(self):
        valid_images = None
        db = votable.parse_single_table(self.database).to_table()
        for key in self.keywords:
            try:
                db_column = np.array(db[key["keyword"]])
                column = db_column.astype(key.get("type", db_column.dtype))

                image_pos = np.where(db["FILENAME"] == self.ref_image)
                ref_value = column[image_pos]

                value = np.array(key.get("constant", ref_value),
                                 dtype=column.dtype)

                operation = key.get("operation", "eq")
                comparison = getattr(column, "__{}__".format(operation))
                new_images = db["FILENAME"][np.where(comparison(value))]

            except AttributeError:
                if operation == "diff":
                    value = np.array(key["constant"], dtype=column.dtype)

                    comparison = np.where(abs(column - ref_value) < value)
                    new_images = db["FILENAME"][comparison]
                else:
                    raise

            except KeyError as err:
                raise err("'keyword' tag is required in keywords")

            if valid_images is None:
                valid_images = set(new_images)
            else:
                valid_images = valid_images & set(new_images)

        if len(valid_images) < self.min_number:
            raise ValueError("Number of images found: {}", len(valid_images))

        with self.output().open("w") as outf:
            for image in valid_images:
                if image != self.ref_image:
                    outf.write("{}\n".format(image))
