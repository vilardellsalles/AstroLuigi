import os.path
from tempfile import gettempdir

import pytest

import luigi

from numpy import zeros, array_equal
import ccdproc

from astroluigi import ccdred


class TestCCDRed:
    """
    CCDRed TestClass
    """

    output_pars = [(False), (""), ("ccdred_output.fits")]

    @pytest.mark.parametrize("use_tmpdir", output_pars)
    def test_ccdred_output(self, tmpdir, use_tmpdir):
        """
        Ensure that CCDRed.output works as expected
        """

        if use_tmpdir:
            full_path = str(tmpdir.join(use_tmpdir))
        else:
            full_path = ""

        test_ccdred = ccdred.CCDRed(output_file=full_path)
    
        test_name = full_path
        test_id = test_ccdred.task_id + ".fit"
        if os.path.isdir(full_path):
            test_name = os.path.join(full_path, test_id)
        elif not full_path:
            test_name = os.path.join(gettempdir(), "astroluigi", test_id)

        assert test_ccdred.output().path == test_name


class TestBiasSubtract:
    """
    BiasSubtract TestClass
    """

    def bias_subtract_run(self, tmpdir, create_FITS):
        """
        Base method to test BiasSubtract.run
        """

        test_bias_subtract = ccdred.BiasSubtract(image=create_FITS[0],
                                                 bias=create_FITS[1])

        luigi.build([test_bias_subtract], local_scheduler=True)

        return test_bias_subtract.output().path

    def test_bias_subtract_run_file(self, tmpdir, create_FITS):
        """
        Ensure that BiasSubtract creates the expected file
        """

        assert os.path.isfile(self.bias_subtract_run(tmpdir, create_FITS))

    def test_bias_subtract_run_content(self, tmpdir, create_FITS):
        """
        Ensure that the BiasSubtract output file has the expected content
        """

        out_path = self.bias_subtract_run(tmpdir, create_FITS)

        content = ccdproc.fits_ccddata_reader(out_path)
        image = ccdproc.fits_ccddata_reader(create_FITS[0])

        assert array_equal(content.data, 2*image.data)


class TestZeroCombine:
    """
    ZeroCombine TestClass
    """

    def zero_combine_run(self, tmpdir, create_FITS):
        """
        Base method to test ZeroCombine.run
        """

        test_zero_combine = ccdred.ZeroCombine(bias_list=create_FITS,
                                               output_file=str(tmpdir))

        luigi.build([test_zero_combine], local_scheduler=True)

        return test_zero_combine.output().path

    def test_zero_combine_run_file(self, tmpdir, create_FITS):
        """
        Ensure that ZeroCombine creates the expected file
        """

        assert os.path.isfile(self.zero_combine_run(tmpdir, create_FITS))

    def test_zero_combine_run_content(self, tmpdir, create_FITS):
        """
        Ensure that the ZeroCombine output file has the expected content
        """

        out_path = self.zero_combine_run(tmpdir, create_FITS)

        content = ccdproc.fits_ccddata_reader(out_path)

        assert array_equal(content.data, zeros([2,2]))


class TestDarkCombine:
    """
    DarkCombine TestClass
    """

    input_pars = [(False), (True)]

    def dark_combine_run(self, tmpdir, create_FITS, use_bias):
        """
        Base method to test DarkCombine.run
        """

        bias = ""
        if use_bias:
            bias = create_FITS[1]

        test_dark_combine = ccdred.DarkCombine(dark_list=create_FITS,
                                               bias=bias,
                                               output_file=str(tmpdir))

        luigi.build([test_dark_combine], local_scheduler=True)

        return test_dark_combine.output().path

    @pytest.mark.parametrize("use_bias", input_pars)
    def test_dark_combine_run_file(self, create_FITS, tmpdir, use_bias):
        """
        Ensure that DarkCombine creates the expected file
        """

        out_path = self.dark_combine_run(tmpdir, create_FITS, use_bias)
        assert os.path.isfile(out_path)

    @pytest.mark.parametrize("use_bias", input_pars)
    def test_dark_combine_run_content(self, tmpdir, create_FITS, use_bias):
        """
        Ensure that the DarkCombine output file has the expected content
        """

        out_path = self.dark_combine_run(tmpdir, create_FITS, use_bias)

        content = ccdproc.fits_ccddata_reader(out_path)

        if use_bias:
            image = ccdproc.fits_ccddata_reader(create_FITS[0])
            exptime = image.header["EXPTIME"]

            test_content = image.data / exptime
        else:
            test_content = zeros([2,2])

        assert array_equal(content.data, test_content)


class TestFlatCombine:
    """
    FlatCombine TestClass
    """

    output_pars = [(""), ("flat.fits"), ("flat_combine_output.fits")]

    @pytest.mark.parametrize("out_name", output_pars)
    def test_flat_combine_run_file(self, create_FITS, tmpdir, out_name):
        """
        Ensure that FlatCombine creates the expected file
        """

        full_path = str(tmpdir.join(out_name))

        test_flat_combine = ccdred.FlatCombine(flat_list=create_FITS,
                                               bias=create_FITS[0],
                                               dark=create_FITS[1],
                                               output_file=full_path)

        luigi.build([test_flat_combine], local_scheduler=True)

        assert os.path.isfile(test_flat_combine.output().path)

    @pytest.mark.parametrize("out_name", output_pars)
    def test_flat_combine_run_content(self, create_FITS, tmpdir, out_name):
        """
        Ensure that the FlatCombine output file has the expected content
        """

        full_path = str(tmpdir.join(out_name))

        test_flat_combine = ccdred.FlatCombine(flat_list=create_FITS,
                                               bias=create_FITS[0],
                                               dark=create_FITS[1],
                                               output_file=full_path)

        luigi.build([test_flat_combine], local_scheduler=True)

        content = ccdproc.fits_ccddata_reader(test_flat_combine.output().path)
        image2 = ccdproc.fits_ccddata_reader(create_FITS[0])
        mean = image2.data.mean()

        assert array_equal(content.data, image2.data / mean)
