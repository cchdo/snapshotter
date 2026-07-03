from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from cchdo.snapshotter.__main__ import initialize_manifest, write_manifest_line


class ManifestTests(unittest.TestCase):
    def test_manifest_reset_on_restart(self):
        with TemporaryDirectory() as directory:
            snapshot = Path(directory)

            initialize_manifest(snapshot)
            write_manifest_line(snapshot, "first.zip,10,hash1")
            write_manifest_line(snapshot, "second.zip,20,hash2")

            # Simulate restarting on the same day.
            initialize_manifest(snapshot)
            write_manifest_line(snapshot, "third.zip,30,hash3")

            content = (snapshot / "_manifest.csv").read_text().splitlines()

            self.assertEqual(
                content,
                ["file,size,sha256", "third.zip,30,hash3"],
            )


if __name__ == "__main__":
    unittest.main()