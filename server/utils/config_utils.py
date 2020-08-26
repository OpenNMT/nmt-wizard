import yaml


class ConfigUtils:
    @staticmethod
    def read_file(file_path):
        with open(file_path, "r") as file:
            # The FullLoader parameter handles the conversion from YAML
            # scalar values to Python the dictionary format
            content = yaml.load(file, Loader=yaml.FullLoader)
            return content

