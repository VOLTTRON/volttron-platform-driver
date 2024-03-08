from volttron.driver.base.interfaces import BaseInterface

class UnknownInterface(BaseInterface):
    def configure(self, config_dict, registry_config_str):
        pass

    def get_point(self, point_name, **kwargs):
        pass

    def set_point(self, point_name, value, **kwargs):
        pass

    def scrape_all(self):
        pass

    def revert_all(self, **kwargs):
        pass

    def revert_point(self, point_name, **kwargs):
        pass

    @classmethod
    def unique_controller_id(cls, equipment_name, config, **kwargs):
        return 'some', 'unique', 'id'
