import unittest

from consumer.app import parse_db_results_to_periods, get_report_from_meds_object
from collections import defaultdict


class MainTests(unittest.IsolatedAsyncioTestCase):

    async def test_parse_db_results_to_periods_simple(self):
        result = await parse_db_results_to_periods([(1, 'X', 'start', 1609459200), (1, 'X', 'stop', 1609462800)])
        med_name = 'X'
        self.assertEqual(list(result.keys())[0], med_name)
        self.assertEqual(result[med_name]['start'], '2021-01-01 00:00:00')
        self.assertEqual(result[med_name]['stop'], '2021-01-01 01:00:00')

    async def test_parse_db_results_to_periods_complex_1(self):
        result = await parse_db_results_to_periods([(5, 'YZ', 'start', 1609473600), (5, 'YZ', 'stop', 1609484400), (5, 'YZ', 'stop', 1609480800)])
        med_name = 'YZ'
        self.assertEqual(list(result.keys())[0], med_name)
        self.assertEqual(result[med_name]['start'], '2021-01-01 04:00:00')
        self.assertEqual(result[med_name]['stop'], '2021-01-01 07:00:00')

    async def test_parse_db_results_to_periods_complex_2(self):
        result = await parse_db_results_to_periods([(6, 'YZ', 'start', 1609473600), (6, 'YZ', 'stop', 1609484400), (6, 'YZ', 'stop', 1609488000)])
        med_name = 'YZ'
        self.assertEqual(list(result.keys())[0], med_name)
        self.assertEqual(result[med_name]['start'], '2021-01-01 04:00:00')
        self.assertEqual(result[med_name]['stop'], '2021-01-01 08:00:00')

    async def test_parse_db_results_to_periods_invalid_action(self):
        result = await parse_db_results_to_periods([(7, 'A', 'start', 1609473600), (7, 'A', 'test', 1609484400)])
        med_name = 'A'
        self.assertEqual(list(result.keys())[0], med_name)
        self.assertEqual(result[med_name]['start'], '2021-01-01 04:00:00')
        self.assertIsNone(result[med_name].get('test'))

    async def test_get_report_from_meds_object_simple(self):
        test_dict = defaultdict(dict)
        test_dict['X'] = {'start': '2021-01-01 00:00:00', 'stop': '2021-01-01 01:00:00'}
        result = await get_report_from_meds_object(test_dict, 1)
        self.assertEqual(result,"Report for p_id 1\nMedician X start time 2021-01-01 00:00:00 and ends 2021-01-01 01:00:00")

    async def test_get_report_from_meds_object_only_start(self):
        test_dict = defaultdict(dict)
        test_dict['Y'] = {'start': '2021-01-01 00:00:00'}
        result = await get_report_from_meds_object(test_dict, 1)
        self.assertEqual(result,"Report for p_id 1\nThe Medician Y have only start time")

    async def test_get_report_from_meds_object_no_med(self):
        test_dict = defaultdict(dict)
        test_dict['Y'] = {}
        result = await get_report_from_meds_object(test_dict, 1)
        self.assertEqual(result,"Didn't found any medicians for p_id 1")



if __name__ == '__main__':
    unittest.main()