import luigi
import ccdproc
import astropy.units as u

from .targets import TempLocalTarget


class CCDRed(luigi.Task):
    """
    Base class for tasks using ccdproc functions
    """

    output_file = luigi.parameter.Parameter(default="", positional=False)

    def output(self):
        # The .fit extension is required to allow ccdproc.combine
        # recognize the output file format

        taskid = self.task_id.split("_")
        hash_value = taskid[0] + "_" + taskid[-1] + ".fit"
        return TempLocalTarget(path=self.output_file, add_hash=hash_value)


class BiasSubtract(CCDRed):
    """
    Task using ccdproc.subtract_bias function
    """

    image = luigi.parameter.Parameter()
    bias = luigi.parameter.Parameter()

    def run(self):
        data = ccdproc.CCDData.read(self.image, unit="adu")
        master_bias = ccdproc.CCDData.read(self.bias, unit="adu")

        sub_data = ccdproc.subtract_bias(data, master_bias, add_keyword=False)

        ccdproc.fits_ccddata_writer(sub_data, self.output().path)


class ZeroCombine(CCDRed):
    """
    Combine a list of bias frames using the ccdproc.combine method
    """

    bias_list = luigi.parameter.ListParameter()
    method = luigi.parameter.Parameter(default="median")

    def run(self):
        ccdproc.combine(list(self.bias_list), method=self.method,
                        output_file=self.output().path, unit="adu")


class DarkCombine(CCDRed):
    """
    Combine a list of dark frames using the ccdproc.combine function.
    A master bias is subtracted to each dark frame and scaled
    by the exposure time in header (EXPTIME, by default)
    before combination
    """

    dark_list = luigi.parameter.ListParameter()
    bias = luigi.parameter.Parameter(default="")
    scale = luigi.parameter.Parameter(default="EXPTIME")
    method = luigi.parameter.Parameter(default="median")

    def run(self):

        if self.bias:
            clean_images = []
            for image in self.dark_list:
                sub_image = yield BiasSubtract(image, self.bias)
                clean_images += [sub_image.path]
        else:
            clean_images = list(self.dark_list)

        read = ccdproc.CCDData.read
        scaling = [1.0 / read(image, unit="adu").header[self.scale]
                   for image in clean_images]

        dark = ccdproc.combine(clean_images, method=self.method, scale=scaling)
        dark.header[self.scale] = 1.0

        ccdproc.fits_ccddata_writer(dark, self.output().path)


class FlatCombine(CCDRed):
    """
    Combine a list of flat frames using the ccdproc combine method.
    A master bias and a master dark is subtracted to each flat frame
    and scaled by their average number of counts before combination
    """

    flat_list = luigi.parameter.ListParameter()
    scale = luigi.parameter.Parameter(default="")
    method = luigi.parameter.Parameter(default="median")
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
