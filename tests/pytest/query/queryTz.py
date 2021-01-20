###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import os
import sys
import time
import taos
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql
from util.dnodes import tdDnodes


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.ts = 1577808000000
        self.conn = conn

    def resetTimezone(self):
        os.system("sudo timedatectl set-ntp true")

        os.system("sudo timedatectl set-timezone \"Asia/Shanghai\"")

        os.system("sudo timedatectl status")

        os.environ['TZ'] = ':Asia/Shanghai'

        time.tzset()

        
    def run(self):
        os.system("sudo timedatectl set-timezone \"Asia/Shanghai\"")
        tdSql.prepare()

        tdSql.execute("create table st (ts timestamp, voltage int) tags (loc nchar(30))")
        for i in range(100):
             tdSql.execute("insert into t0 using st tags ('beijing') values(%d, %d)" % (self.ts + i * 24 * 3600000, i+1))
                
        os.system("sudo timedatectl status")

        tdSql.query("select sum(voltage) from t0 interval(1n)")

        #self.resetTimezone()

        tdSql.checkRows(4)        
        tdSql.checkData(0, 0, "2020-01-01 00:00:00")
        tdSql.checkData(0, 1, 496)
        tdSql.checkData(1, 0, "2020-02-01 00:00:00")
        tdSql.checkData(1, 1, 1334)
        tdSql.checkData(2, 0, "2020-03-01 00:00:00")
        tdSql.checkData(2, 1, 2356)
        tdSql.checkData(3, 0, "2020-04-01 00:00:00")
        tdSql.checkData(3, 1, 864)

        os.system("sudo timedatectl set-ntp false")

        os.system("sudo timedatectl set-timezone UTC")

        os.system("sudo timedatectl status")

        os.environ['TZ'] = 'UTC'

        time.tzset()

        tdSql.query("select sum(voltage) from t0 interval(1n)")

        #self.resetTimezone()

        tdSql.checkRows(5)        
        tdSql.checkData(0, 0, "2019-12-01 00:00:00")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, "2020-01-01 00:00:00")
        tdSql.checkData(1, 1, 527)
        tdSql.checkData(2, 0, "2020-02-01 00:00:00")
        tdSql.checkData(2, 1, 1363)
        tdSql.checkData(3, 0, "2020-03-01 00:00:00")
        tdSql.checkData(3, 1, 2387)
        tdSql.checkData(4, 0, "2020-04-01 00:00:00")
        tdSql.checkData(4, 1, 772)


        os.system("sudo timedatectl set-timezone \"America/Adak\"")

        os.system("sudo timedatectl status")

        os.environ['TZ'] = ':America/Adak'

        time.tzset()

        tdSql.query("select sum(voltage) from t0 interval(1n)")

        #self.resetTimezone()

        tdSql.checkRows(5)        
        tdSql.checkData(0, 0, "2019-12-01 00:00:00")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, "2020-01-01 00:00:00")
        tdSql.checkData(1, 1, 527)
        tdSql.checkData(2, 0, "2020-02-01 00:00:00")
        tdSql.checkData(2, 1, 1363)
        tdSql.checkData(3, 0, "2020-03-01 00:00:00")
        tdSql.checkData(3, 1, 2387)
        tdSql.checkData(4, 0, "2020-04-01 00:00:00")
        tdSql.checkData(4, 1, 772)




        os.system("sudo timedatectl set-timezone \"Indian/Cocos\"")

        os.system("sudo timedatectl status")

        os.environ['TZ'] = ':Indian/Cocos'

        time.tzset()

        tdSql.query("select sum(voltage) from t0 interval(1n)")

        #self.resetTimezone()

        tdSql.checkRows(4)        
        tdSql.checkData(0, 0, "2020-01-01 00:00:00")
        tdSql.checkData(0, 1, 496)
        tdSql.checkData(1, 0, "2020-02-01 00:00:00")
        tdSql.checkData(1, 1, 1334)
        tdSql.checkData(2, 0, "2020-03-01 00:00:00")
        tdSql.checkData(2, 1, 2356)
        tdSql.checkData(3, 0, "2020-04-01 00:00:00")
        tdSql.checkData(3, 1, 864)



        os.system("sudo timedatectl set-ntp true")

        os.system("sudo timedatectl set-timezone \"Asia/Shanghai\"")

        os.system("sudo timedatectl status")

        os.environ['TZ'] = ':Asia/Shanghai'

        time.tzset()


    def stop(self):
        os.system("sudo timedatectl set-ntp true")

        os.system("sudo timedatectl set-timezone \"Asia/Shanghai\"")

        os.system("sudo timedatectl status")

        os.environ['TZ'] = ':Asia/Shanghai'

        time.tzset()

        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
