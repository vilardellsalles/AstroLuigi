import os.path
from tempfile import gettempdir

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


class TempLocalTarget(luigi.file.LocalTarget):
    def __init__(self, path=None, format=None, add_hash=""):

        if path and not os.path.isdir(path):
            add_hash = ""
        elif not path:
            parent_dir = os.path.basename(os.path.dirname(__file__))
            path = os.path.join(gettempdir(), parent_dir)

        if add_hash:
            path = os.path.join(path, add_hash)

        # Ensure that destination directory exists

        tmp_path = os.path.dirname(path)
        if tmp_path:
            os.makedirs(tmp_path, exist_ok=True)

        # Due to Luigi issue #1519, we cannot use is_tmp=True

        super().__init__(path, format)
