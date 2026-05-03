import unittest

from gf_brew_flow import is_boil_step, split_mash_and_boil_steps

class SplitStepsTest(unittest.TestCase):
    def test_detects_boil_by_keyword(self):
        steps = [
            {"name": "Einmaischen", "target_temp": 63},
            {"name": "Maltoserast", "target_temp": 63},
            {"name": "Würzekochen", "target_temp": 100},
            {"name": "Whirlpool", "target_temp": None},
        ]
        mash, boil = split_mash_and_boil_steps(steps)
        self.assertEqual([step["name"] for step in mash], ["Einmaischen", "Maltoserast"])
        self.assertEqual([step["name"] for step in boil], ["Würzekochen", "Whirlpool"])

    def test_uses_pump_action_to_close_mash(self):
        steps = [
            {"name": "Einmaischen", "target_temp": 63},
            {"name": "Abmaischen", "target_temp": 78, "pump_action": "stop"},
            {"name": "Würzekochen", "target_temp": 100},
        ]
        mash, boil = split_mash_and_boil_steps(steps)
        self.assertEqual([step["name"] for step in mash], ["Einmaischen", "Abmaischen"])
        self.assertEqual([step["name"] for step in boil], ["Würzekochen"])

class BoilDetectionTest(unittest.TestCase):
    def test_detects_wuerzekochen(self):
        self.assertTrue(is_boil_step({"name": "Würzekochen", "target_temp": 100}))

    def test_detects_boiling_by_temp(self):
        self.assertTrue(is_boil_step({"name": "Sieden", "target_temp": 101}))

    def test_ignores_mash_only(self):
        self.assertFalse(is_boil_step({"name": "Schrotrasten", "target_temp": 65}))

if __name__ == "__main__":
    unittest.main()
