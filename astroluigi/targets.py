import os.path
import warnings

import luigi


class ASCIITarget(luigi.file.LocalTarget):
    @property
    def content(self):
        file_content = []
        if self.exists():
            with self.open() as tmpf:
                for line in tmpf:
                    file_content += [line.strip()]

        return file_content


class HashTarget(luigi.file.LocalTarget):
    def __init__(self, path=None, format=None, is_tmp=False, add_hash=False):
        msg = "HashTarget superseded by CCDRed.output method."
        warnings.warn(msg, DeprecationWarning)

        if add_hash and path and not is_tmp:
            base, ext = os.path.splitext(path)
            path = "{}_{}{}".format(base, add_hash, ext)

        super().__init__(path, format, is_tmp)
