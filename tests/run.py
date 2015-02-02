#!/usr/bin/env python
# coding=utf-8
#
#  Copyright (c) 2013-2015 First Flamingo Enterprise B.V.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
#  run.py
#  firstflamingo/treinenaapje
#
#  Created by Berend Schotanus on 11-Jan-13.
#

import sys, unittest

def main():
    if len(sys.argv) == 2:
        moduleName = sys.argv[1]
    else:
        moduleName = '*'
    pattern = 'Test' + moduleName + '.py'
    
    sys.path.insert(0, SDK_PATH)
    sys.path.insert(0, CODE_PATH)
    import dev_appserver
    dev_appserver.fix_sys_path()
    suite = unittest.loader.TestLoader().discover(TEST_PATH, pattern=pattern)
    unittest.TextTestRunner(verbosity=2).run(suite)

if __name__ == '__main__':
    main()
