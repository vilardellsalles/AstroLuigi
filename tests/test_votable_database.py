import os.path

from astropy.io import votable


class TestCreateDB:
    """
    CreateDB TestClass
    """

    def test_create_db_output(self, tmpdir, create_database):
        """
        Ensure that CreateDB.output works as expected
        """

        full_path = str(tmpdir.join("database_output.xml"))

        assert full_path == create_database

    def test_create_db_run_file(self, create_database):
        """
        Ensure that CreateDB creates the expected file
        """

        assert os.path.isfile(create_database)

    def test_create_db_run_content_schema(self, create_database):
        """
        Ensure that the created database conforms to VOTable schema
        """

        ret_code = votable.xmlutil.validate_schema(create_database,
                                                   version="1.2")

        assert ret_code[0] == 0

    def test_create_db_run_content_fields(self, create_database):
        """
        Ensure that the created database has the proper
        number of columns
        """

        test_types = ["unicodeChar", "long", "bit", "long", "long", "long",
                      "long", "double", "unicodeChar"]

        test_table = votable.parse_single_table(create_database)
        field_types = [field.datatype for field in test_table.fields]

        assert test_types == field_types

    def test_create_db_run_content_values(self, create_database):
        """
        Ensure that the created database has the proper
        number of columns
        """

        test_values = [0, True, 64, 2, 2, 2, 10.0, "adu"]

        test_table = votable.parse_single_table(create_database)
        table_values = test_table.array.data

        assert all(list(row)[1:] == test_values for row in table_values)


class TestImCalib:
    """
    ImCalib TestClass
    """

    def test_im_calib_output(self, tmpdir, create_list):
        """
        Ensure that ImCalib.output works as expected
        """

        full_path = str(tmpdir.join("imcalib.lst"))

        assert full_path == create_list

    def test_im_calib_run_file(self, create_list):
        """
        Ensure that ImCalib creates the expected file
        """

        assert os.path.isfile(create_list)

    def test_im_calib_run_content(self, create_FITS, create_list):
        """
        Ensure that ImCalib output list has the expected content
        """

        image_list = []
        with open(create_list, "r") as test_list:
            for image in test_list:
                image_list += [image.strip()]

        assert image_list == create_FITS[1:]
