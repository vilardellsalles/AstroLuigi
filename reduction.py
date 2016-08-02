import sys
import os.path

import luigi


class Reduction(luigi.WrapperTask):
    obsdata = luigi.parameter.Parameter()
    extension = luigi.parameter.Parameter(default="imr.fits")

    def requires(self):
        task_list = []
        for path, subdirs, files in os.walk(self.obsdata):
            for image in files:
                if image.endswith(self.extension):
                    image_name = os.path.join(path, image)
                    task_list += [ImageReduction(image=image_name)]

        return task_list


class ImageReduction(luigi.WrapperTask):
    image = luigi.parameter.Parameter()

    def requires(self):
        bias = yield CalibrationImages(image=self.image,
                                       keywords={"OBSTYPE": "Bias"})


class CalibrationImages(luigi.Task):
    image = luigi.Parameter.Parameter()
    keywords = luigi.parameter.DictParameter()

    def run(self):
        
    def output(self):
        root, extension = os.path.splitext(self.image)
        
        return luigi.file.LocalTarget(root + ".lst")
    

if __name__ == "__main__":

    luigi.run(main_task_cls=Reduction)
