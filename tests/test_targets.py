import os.path
from tempfile import gettempdir

import pytest

from astroluigi import targets


def test_ascii_target_content(tmpdir):
    """
    Create a file and ensure that ASCIITarget.content provides it
    """
    test_content = ["one", "two", "three"]

    tmpfile = tmpdir.join("ascii_target_content.txt")
    with tmpfile.open("w") as tmpf:
        for line in test_content:
            tmpf.write("{}\n".format(line))

    test_target = targets.ASCIITarget(tmpfile.strpath)

    assert test_content == test_target.content


target_init_pars = [(False), (""), ("test.txt")]
@pytest.mark.parametrize("use_tmpdir", target_init_pars)
def test_temp_local_target_init(tmpdir, use_tmpdir):
    """
    Check whether TempLocalTarget produces the proper file names
    """

    if use_tmpdir:
        full_path = str(tmpdir.join(use_tmpdir))
    else:
        full_path = ""
    
    hash_value = "test.txt"
    test_target = targets.TempLocalTarget(full_path, add_hash=hash_value).path

    test_name = full_path
    if os.path.isdir(full_path):
        test_name = os.path.join(full_path, hash_value)
    elif not full_path:
        test_name = os.path.join(gettempdir(), "astroluigi", hash_value)

    assert test_name == test_target
