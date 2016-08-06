import os.path

from numpy import zeros, array_equal
import ccdproc
import pytest

from astroluigi import ccdred


class TestZeroCombine:
    """
    ZeroCombine TestClass
    """

    output_pars = [(""), ("zero_combine_output.fits")]

    @pytest.mark.parametrize("out_name", output_pars)
    def test_zero_combine_output(self, create_FITS, out_name):
        """
        Ensure that ZeroCombine.output works as expected
        """
    
        test_zero_combine = ccdred.ZeroCombine(bias_list=create_FITS,
                                               output_file=out_name)
    
        base, ext = os.path.splitext(test_zero_combine.output().path)
    
        if out_name:
            new_base = base
            test_path = out_name
        else:
            new_base = "".join(base.split("_")[:-1])
            test_path = "bias.fits"
    
        assert test_path == "{}{}".format(new_base, ext)

    @pytest.mark.parametrize("out_name", output_pars)
    def test_zero_combine_run_file(self, create_FITS, tmpdir, out_name):
        """
        Ensure that ZeroCombine creates the expected file
        """

        full_path = str(tmpdir.join(out_name))

        test_zero_combine = ccdred.ZeroCombine(bias_list=create_FITS,
                                               output_file=full_path)

        test_zero_combine.run()

        assert os.path.isfile(test_zero_combine.output().path)

    @pytest.mark.parametrize("out_name", output_pars)
    def test_zero_combine_run_content(self, create_FITS, tmpdir, out_name):
        """
        Ensure that ZeroCombine creates the expected file
        """

        full_path = str(tmpdir.join(out_name))

        test_zero_combine = ccdred.ZeroCombine(bias_list=create_FITS,
                                               output_file=full_path)

        test_zero_combine.run()

        content = ccdproc.fits_ccddata_reader(test_zero_combine.output().path)

        assert array_equal(content.data, zeros([2,2]))


class TestDarkCombine:
    """
    DarkCombine TestClass
    """

    output_pars = [(""), ("dark_combine_output.fits")]

    @pytest.mark.parametrize("out_name", output_pars)
    def test_dark_combine_output(self, create_FITS, out_name):
        """
        Ensure that DarkCombine.output works as expected
        """
    
        test_dark_combine = ccdred.DarkCombine(dark_list=create_FITS,
                                               bias=create_FITS[0],
                                               output_file=out_name)
    
        base, ext = os.path.splitext(test_dark_combine.output().path)
    
        if out_name:
            new_base = base
            test_path = out_name
        else:
            new_base = "".join(base.split("_")[:-1])
            test_path = "dark.fits"
    
        assert test_path == "{}{}".format(new_base, ext)

    @pytest.mark.parametrize("out_name", output_pars)
    def test_dark_combine_run_file(self, create_FITS, tmpdir, out_name):
        """
        Ensure that DarkCombine creates the expected file
        """

        full_path = str(tmpdir.join(out_name))

        test_dark_combine = ccdred.DarkCombine(dark_list=create_FITS,
                                               bias=create_FITS[0],
                                               output_file=full_path)

        test_dark_combine.run()

        assert os.path.isfile(test_dark_combine.output().path)

    @pytest.mark.parametrize("out_name", output_pars)
    def test_dark_combine_run_content(self, create_FITS, tmpdir, out_name):
        """
        Ensure that DarkCombine creates the expected file
        """

        full_path = str(tmpdir.join(out_name))

        test_dark_combine = ccdred.DarkCombine(dark_list=create_FITS,
                                               bias=create_FITS[0],
                                               output_file=full_path)

        test_dark_combine.run()

        content = ccdproc.fits_ccddata_reader(test_dark_combine.output().path)
        image2 = ccdproc.fits_ccddata_reader(create_FITS[1])
        exptime = image2.header["EXPTIME"]
        

        assert array_equal(content.data, image2.data / exptime)

