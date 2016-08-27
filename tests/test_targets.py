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


target_init_pars = [(False), ("test")]
@pytest.mark.parametrize("add_hash", target_init_pars)
def test_hash_target_init(add_hash):
    """
    Check whether HashTarget produces the proper file names
    """
    basename = "hash_target_init"

    filename = "{}.txt".format(basename)
    test_name = targets.HashTarget(filename, add_hash=add_hash).path

    test_hash = basename
    if add_hash:
        test_hash = "{}_{}".format(basename, add_hash)

    assert test_name == "{}.txt".format(test_hash)
