import luigi
import ccdproc

from targets import HashTarget


class ZeroCombine(luigi.Task):
    bias_list = luigi.parameter.ListParameter()
    method = luigi.parameter.Parameter(default="median")
    output_file = luigi.parameter.Parameter(default="")

    def output(self):
        if self.output_file:
            return HashTarget(self.output_file)
        else:
            return HashTarget("bias_", add_hash=self.bias_list)

    def run(self):
        ccdproc.combine(list(self.bias_list), method=self.method,
                        output_file=self.output().path, unit="adu")


class DarkCombine(luigi.Task):
    dark_list = luigi.parameter.ListParameter()
    method = luigi.parameter.Parameter(default="median")
    scale = luigi.parameter.Parameter(default="EXPTIME")
    bias = luigi.parameter.Parameter(default="")
    output_file = luigi.parameter.Parameter(default="")

    def output(self):
        if self.output_file:
            return HashTarget(self.output_file)
        else:
            return HashTarget("dark_", add_hash=self.dark_list)

    def run(self):
        data = [ccdproc.CCDData.read(image, unit="adu")
                for image in self.dark_list]

        if self.bias:
            bias = ccdproc.CCDData.read(self.bias, unit="adu")
            data = [ccdproc.subtract_bias(image, bias) for image in data]

        scaling = [1.0 / image.header[self.scale] for image in data]
        dark = ccdproc.combine(data, method=self.method, scale=scaling)
        dark.header[self.scale] = 1.0

        ccdproc.fits_ccddata_writer(dark, self.output().path)
