import json

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
        if add_hash and path and not is_tmp:
            path = "{}{:x}.fits".format(path, abs(hash(json.dumps(add_hash))))

        super(HashTarget, self).__init__(path, format, is_tmp)
