import os.path

import luigi
import ccdproc
import astropy.units as u

from .targets import HashTarget


class ZeroCombine(luigi.Task):
    """
    Combine a list of bias frames using the ccdproc combine method
    """

    bias_list = luigi.parameter.ListParameter()
    method = luigi.parameter.Parameter(default="median")
    output_file = luigi.parameter.Parameter(default="")

    def output(self):
        if os.path.isdir(self.output_file) or not self.output_file:
            out_path = os.path.join(self.output_file, "bias.fits")
            hash_value = self.task_id.split("_")[-1]
            return HashTarget(out_path, add_hash=hash_value)
        else:
            return HashTarget(self.output_file)

    def run(self):
        ccdproc.combine(list(self.bias_list), method=self.method,
                        output_file=self.output().path, unit="adu")


class DarkCombine(luigi.Task):
    """
    Combine a list of dark frames using the ccdproc combine method.
    A master bias is subtracted to each dark frame and scaled
    by the exposure time in header (EXPTIME, by default)
    before combination
    """

    dark_list = luigi.parameter.ListParameter()
    method = luigi.parameter.Parameter(default="median")
    scale = luigi.parameter.Parameter(default="EXPTIME")
    bias = luigi.parameter.Parameter(default="")
    output_file = luigi.parameter.Parameter(default="")

    def output(self):
        if os.path.isdir(self.output_file) or not self.output_file:
            out_path = os.path.join(self.output_file, "dark.fits")
            hash_value = self.task_id.split("_")[-1]
            return HashTarget(out_path, add_hash=hash_value)
        else:
            return HashTarget(self.output_file)

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


class FlatCombine(luigi.Task):
    """
    Combine a list of flat frames using the ccdproc combine method.
    A master bias and a master dark is subtracted to each flat frame
    and scaled by their average number of counts before combination
    """

    flat_list = luigi.parameter.ListParameter()
    method = luigi.parameter.Parameter(default="median")
    scale = luigi.parameter.Parameter(default="")
    bias = luigi.parameter.Parameter(default="")
    dark = luigi.parameter.Parameter(default="")
    output_file = luigi.parameter.Parameter(default="")

    def output(self):
        if os.path.isdir(self.output_file) or not self.output_file:
            out_path = os.path.join(self.output_file, "flat.fits")
            hash_value = self.task_id.split("_")[-1]
            return HashTarget(out_path, add_hash=hash_value)
        else:
            return HashTarget(self.output_file)

    def run(self):
        data = [ccdproc.CCDData.read(image, unit="adu")
                for image in self.flat_list]

        if self.bias:
            bias = ccdproc.CCDData.read(self.bias, unit="adu")
            bdata = [ccdproc.subtract_bias(image, bias) for image in data]

        if self.dark:
            dark = ccdproc.CCDData.read(self.dark, unit="adu")
            ddata = [ccdproc.subtract_dark(image, dark,
                               exposure_time="EXPTIME", exposure_unit=u.second,
                               scale=True) for image in bdata]

        scaling = [1 / image.data.mean() for image in ddata]
        flat = ccdproc.combine(ddata, method=self.method, scale=scaling)

        ccdproc.fits_ccddata_writer(flat, self.output().path)
