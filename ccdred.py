import json

import luigi
import ccdproc

class ZeroCombine(luigi.Task):
    bias_list = luigi.parameter.ListParameter()
    method = luigi.parameter.Parameter(default="median")
    output_file = luigi.parameter.Parameter(default="")

    def output(self):
        if self.output_file:
            return luigi.file.LocalTarget(self.output_file)
        else:
            filename = abs(hash(json.dumps(self.bias_list)))
            return luigi.file.LocalTarget("bias_{:x}.fits".format(filename))

    def run(self):
        ccdproc.combine(list(self.bias_list), method=self.method,
                        output_file=self.output().path, unit="adu")
                
