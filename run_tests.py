#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""跑单元测试：python run_tests.py

代码在 app/ 里，测试在 tests/ 里，这里把 app/ 加进 sys.path 再跑。
"""
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "app"))

if __name__ == "__main__":
    suite = unittest.defaultTestLoader.discover(
        os.path.join(HERE, "tests"), top_level_dir=os.path.join(HERE, "tests"))
    r = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if r.wasSuccessful() else 1)
