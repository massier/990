from enum import Enum

irs_bucket_name = 'irs-form-990'
scratch_folder = '../Scratch/'
compat_test_filing = '201533179349307343_public.xml'
official_test_filing = '201541349349307794_public.xml'
xml_ns = {'ns': 'http://www.irs.gov/efile'}


class NodeStatus(Enum):
    attribute = 1
    element = 2
    not_found = 3
