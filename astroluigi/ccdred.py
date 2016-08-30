import os.path

import luigi
import ccdproc
import astropy.units as u


class Combine(luigi.Task):
    """
    Base class for tasks using ccdproc.combine method
    """

    _base_output = "combine.fits"

    method = luigi.parameter.Parameter(default="median")
    output_file = luigi.parameter.Parameter(default=_base_output)

    def output(self):
        out_path = self.output_file

        if os.path.isdir(out_path):
            out_path = os.path.join(self.output_file, self._base_output)

        if os.path.basename(out_path) == self._base_output:
            hash_value = self.task_id.split("_")[-1]
            base, ext = os.path.splitext(out_path)
            out_path = "{}_{}{}".format(base, hash_value, ext)

        return luigi.file.LocalTarget(out_path)


class ZeroCombine(Combine):
    """
    Combine a list of bias frames using the ccdproc combine method
    """

    _base_output = "bias.fits"

    bias_list = luigi.parameter.ListParameter()

    def run(self):
        ccdproc.combine(list(self.bias_list), method=self.method,
                        output_file=self.output().path, unit="adu")


class DarkCombine(Combine):
    """
    Combine a list of dark frames using the ccdproc combine method.
    A master bias is subtracted to each dark frame and scaled
    by the exposure time in header (EXPTIME, by default)
    before combination
    """

    _base_output = "dark.fits"

    dark_list = luigi.parameter.ListParameter()
    scale = luigi.parameter.Parameter(default="EXPTIME")
    bias = luigi.parameter.Parameter(default="")

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


class FlatCombine(Combine):
    """
    Combine a list of flat frames using the ccdproc combine method.
    A master bias and a master dark is subtracted to each flat frame
    and scaled by their average number of counts before combination
    """

    _base_output = "flat.fits"

    flat_list = luigi.parameter.ListParameter()
    scale = luigi.parameter.Parameter(default="")
    bias = luigi.parameter.Parameter(default="")
    dark = luigi.parameter.Parameter(default="")

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
