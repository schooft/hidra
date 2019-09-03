import unittest
import subprocess
import tempfile
import pathlib
import time
from hidra.utils import load_config, write_config


def waitfor(predicate, timeout=5):
    """Wait until predicate is true or timeout seconds passed"""
    if predicate():
        return True

    start = time.time()

    while time.time() - start < timeout:
        time.sleep(min(0.1, timeout - (time.time() - start)))
        if predicate():
            return True

    return predicate()


class TestHidraBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def initClass(cls, remove_files="with_confirmation"):
        cls.tmp = None
        cls.log_dir = None
        cls.target_dir = None
        cls.source_dir = None
        cls.receiver_proc = None
        cls.sender_proc = None

        cls.tmp = pathlib.Path(tempfile.mkdtemp(prefix="hidra_test_"))
        print("tmp =", cls.tmp)
        cls.log_dir = cls.tmp / "log"
        cls.log_dir.mkdir()
        cls.target_dir = cls.tmp / "target"
        cls.target_dir.mkdir()
        cls.source_dir = cls.tmp / "source"
        cls.source_dir.mkdir()

        receiver_config = load_config("conf/datareceiver.yaml")
        receiver_config["general"]["log_path"] = str(cls.log_dir)
        receiver_config["general"]["whitelist"] = None
        receiver_config["datareceiver"]["target_dir"] = str(cls.target_dir)
        receiver_config["datareceiver"]["data_stream_ip"] = "127.0.0.1"

        receiver_config_file = cls.tmp / "datareceiver.yaml"
        write_config(str(receiver_config_file), receiver_config)

        event_type = "inotify_events"
        sender_config = load_config("conf/datamanager.yaml")
        sender_config["general"]["log_path"] = str(cls.log_dir)
        sender_config["general"]["whitelist"] = None
        sender_config["eventdetector"]["type"] = event_type
        sender_config["eventdetector"][event_type]["monitored_dir"] = str(
            cls.source_dir)
        sender_config["eventdetector"][event_type]["create_fix_subdirs"] = True
        sender_config["datafetcher"]["use_data_stream"] = True
        sender_config["datafetcher"]["remove_data"] = remove_files
        sender_config["datafetcher"]["data_stream_targets"] = [
            ["127.0.0.1", 50100]]

        sender_config_file = cls.tmp / "datamanager.yaml"
        write_config(str(sender_config_file), sender_config)

        cls.receiver_proc = subprocess.Popen([
            "hidra_receiver",
            "--config", str(receiver_config_file),
            "--verbose",
            "--onscreen", "debug"
        ])

        cls.sender_proc = subprocess.Popen([
            "hidra_sender",
            "--config", str(sender_config_file),
            "--verbose",
            "--onscreen", "debug"
        ])

        subdir = "commissioning/raw"
        (cls.target_dir / subdir).mkdir(parents=True)
        assert waitfor((cls.source_dir / subdir).is_dir)

    @classmethod
    def tearDownClass(cls):
        if cls.sender_proc:
            cls.sender_proc.terminate()
        if cls.receiver_proc:
            cls.receiver_proc.terminate()
        if cls.sender_proc:
            cls.sender_proc.wait()
        if cls.receiver_proc:
            cls.receiver_proc.wait()


class TestHidraWithConfirmation(TestHidraBase):
    @classmethod
    def setUpClass(cls):
        cls.initClass()

    def test_small_file(self):
        filename = "small.txt"
        content = "foo"
        subdir = pathlib.Path("commissioning/raw")
        source_file = self.source_dir / subdir / filename
        source_file.write_text(content)
        target_file = self.target_dir / subdir / filename

        self.assertTrue(waitfor(target_file.is_file, 10))
        self.assertTrue(target_file.read_text() == content)
        self.assertFalse(source_file.exists())

    def test_big_file(self):
        filename = "large.dat"
        size = 20000000
        content = b"a" * size
        subdir = pathlib.Path("commissioning/raw")
        source_file = self.source_dir / subdir / filename
        source_file.write_bytes(content)
        target_file = self.target_dir / subdir / filename

        self.assertTrue(waitfor(target_file.is_file, 10))
        self.assertTrue(target_file.read_bytes() == content)
        self.assertFalse(source_file.exists())


class TestHidraWithoutRemoval(TestHidraBase):
    @classmethod
    def setUpClass(cls):
        cls.initClass(remove_files=False)

    def test_small_file(self):
        filename = "small.txt"
        content = "foo"
        subdir = pathlib.Path("commissioning/raw")
        source_file = self.source_dir / subdir / filename
        source_file.write_text(content)
        target_file = self.target_dir / subdir / filename

        self.assertTrue(waitfor(target_file.is_file, 10))
        self.assertTrue(target_file.read_text() == content)
        self.assertTrue(source_file.exists())

    def test_big_file(self):
        filename = "large.dat"
        size = 20000000
        content = b"a" * size
        subdir = pathlib.Path("commissioning/raw")
        source_file = self.source_dir / subdir / filename
        source_file.write_bytes(content)
        target_file = self.target_dir / subdir / filename

        self.assertTrue(waitfor(target_file.is_file, 10))
        self.assertTrue(target_file.read_bytes() == content)
        self.assertTrue(source_file.exists())


if __name__ == "__main__":
    unittest.main()
