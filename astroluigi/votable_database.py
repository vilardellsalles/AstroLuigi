import os.path
import fnmatch

import luigi
from astropy import table
from astropy.io import votable
from ccdproc import ImageFileCollection

from .targets import ASCIITarget


class CreateDB(luigi.Task):
    """
    Create a VO table database from a list of FITS files in a
    specific location. The search for fits images can be made recusive
    with recursive parameter set to True (the default)
    """

    location = luigi.parameter.Parameter()
    recursive = luigi.parameter.BoolParameter(default=True)
    database = luigi.parameter.Parameter()

    def output(self):
        return luigi.file.LocalTarget(self.database)

    def run(self):
        # Ideally, this method could be implemented using 
        # ImageFileCollection only. Unfortunately, ImageFileCollection
        # is not recursive and is not able to work with multiple
        # FITS extensions (multiple extensions is not implemented, yet)

        imtable = None
        for path, subdirs, files in os.walk(self.location):
            dirtable = ImageFileCollection(path).summary
            path_list = [os.path.join(path, image) 
                         for image in dirtable["file"]]
            dirtable.replace_column("file", path_list)
            if imtable:
                imtable = table.join(imtable, dirtable, join="outer")
            else:
                imtable = dirtable

            if not self.recursive:
                break

        out_table = votable.from_table(imtable)
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
                db_column = db[key["keyword"]]
                column = db_column.astype(key.get("type", db_column.dtype))

                image_pos = db["file"] == self.ref_image
                ref_value = column[image_pos][0]

                value = table.Column(key.get("constant", ref_value),
                                     dtype=column.dtype)

                operation = key.get("operation", "eq")
                comparison = getattr(column, "__{}__".format(operation))
                new_images = db["file"][comparison(value)]

            except AttributeError:
                if operation == "diff":
                    value = table.Column(key["constant"], dtype=column.dtype)
                    new_images = db["file"][abs(column - ref_value) < value]
                else:
                    raise

            except IndexError:
                valid_images = {}
                break

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
