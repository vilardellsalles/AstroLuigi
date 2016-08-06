import pytest

from numpy import array
import ccdproc

from astroluigi import votable_database as vodb


@pytest.fixture
def create_FITS(tmpdir):
    """
    Function to create FITS images for testing
    """
    test_array = array([[10,20], [30,40]])
    image1 = ccdproc.CCDData(test_array, unit="adu")
    image2 = ccdproc.CCDData(-test_array, unit="adu")

    image1.header = {"EXPTIME": 10.0}
    image2.header = {"EXPTIME": 10.0}

    image1_path = str(tmpdir.join("image1.fits"))
    image2_path = str(tmpdir.join("image2.fits"))

    image1_fits = ccdproc.fits_ccddata_writer(image1, image1_path)
    image2_fits = ccdproc.fits_ccddata_writer(image2, image2_path)

    return [image1_path, image2_path]


@pytest.fixture
def create_database(tmpdir, create_FITS):
    """
    Function to create a VOTable database from created test FITS
    """

    full_path = str(tmpdir.join("database_output.xml"))

    test_create_db = vodb.CreateDB(image_list=create_FITS,
                                   database=full_path)

    test_create_db.run()

    return test_create_db.output().path


@pytest.fixture
def create_list(tmpdir, create_FITS, create_database):
    """
    Function to create a list of images from a VOTable database
    and test FITS
    """

    full_path = str(tmpdir.join("imcalib.lst"))

    test_keywords = [{"keyword": "NAXIS"}]
    test_keywords += [{"keyword": "NAXIS1", "type": "int"}]
    test_keywords += [{"keyword": "NAXIS2", "constant": 2}]
    test_keywords += [{"keyword": "EXPTIME", "constant": 1.0,
                       "type": "float", "operation": "diff"}]

    test_im_calib = vodb.ImCalib(ref_image=create_FITS[0],
                                 database=create_database,
                                 keywords=test_keywords,
                                 image_list=full_path)

    test_im_calib.run()

    return test_im_calib.output().path
