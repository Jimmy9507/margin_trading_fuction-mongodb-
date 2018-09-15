from optparse import OptionParser
from datetime import datetime, timedelta
from dateutil.parser import parse

from datahub.store.stockstop.stockstop import StoringStopStock
from datahub.store.update.update import init_update
from .store.tushare.index_comp import StoringIndexComp
from .config import get_config
from .store.fenji import fenji
from .store.tushare.equity_info import StoringST
from .store.calendar.tradedates import StoringCalendar
from .store.ex_factor.ex_factor import StoringExFactor
from .store.xueqiu.xueqiu import StoringXueqiu
from .store.shenwan.shenwan import StoringShenwan
from .store.share.share import StoringShare
from .store.tmall.tmall import StoringTmall


def run():
    """
    datahub command line entry point
    :return:
    """
    options = parse_arguments()
    component = options.component
    config = get_config(options.config_file)
    init_update(config['update'])

    if not config.get("calendar"):
        raise RuntimeError("Calendar configure must be set.")

    if not component or component == "fenji":
        fenji_dict = config['fenji']
        fenji.StoringFenji(fenji_dict).save()

    if not component or component == "st_stock":
        st = config['st_stock']

        sst = StoringST(st)
        sst.save(False)

    if not component or component == "calendar":
        cal_config = config["calendar"]
        cl = StoringCalendar(cal_config)
        cl.save()

    if not component or component == "index_comp":
        index_comp_config = config["index_comp"]
        cal_config = config["calendar"]
        ic = StoringIndexComp(index_comp_config, cal_config)
        ic.save(False)

    if not component or component == "ex_factor":
        factor_config = config["ex_factor"]
        factor = StoringExFactor(factor_config)
        factor.save()

    if not component or component == "share":
        share_config = config["share"]
        share_config["start_date"] = parse(options.start_date)
        share = StoringShare(share_config)
        share.save()

    if component == "xueqiu":
        xueqiu_config = config["xueqiu"]
        xueqiu_config["start_date"] = options.start_date
        xueqiu_config["end_date"] = options.end_date
        xueqiu = StoringXueqiu(xueqiu_config)
        xueqiu.save()

    if component == "shenwan":
        shenwan_config = config["shenwan"]
        cal_config = config["calendar"]
        shenwan_config["start_date"] = parse(options.start_date)
        shenwan_config["end_date"] = parse(options.end_date)
        shenwan = StoringShenwan(shenwan_config, cal_config)
        shenwan.save()

    if not component or component == "tmall":
        tmall_config = config["tmall"]
        tmall_config["start_date"] = parse(options.start_date)
        tmall_config["end_date"] = parse(options.end_date)
        tmall = StoringTmall(tmall_config)
        tmall.save()

    if not component or component == "stk_stop":
        stop = StoringStopStock(config)
        stop.save()



def parse_arguments():
    parser = OptionParser(usage="usage: datahub -c config.json -d true")
    parser.add_option("-c", "--config", dest="config_file", metavar="FILE",
                      help="/path/to/config_file_name")
    parser.add_option("-d", "--debug", action="store_true", dest="verbose", default=False)
    parser.add_option("-p", "--component", dest="component", help="fenji/st_stock/calendar/index_comp/ex_factor/xueqiu")
    parser.add_option("-s", "--start", dest="start_date", default=(datetime.today()-timedelta(3)).strftime("%Y%m%d"))
    parser.add_option("-e", "--end", dest="end_date", default=datetime.today().strftime("%Y%m%d"))
    options, ignore = parser.parse_args()
    if not options.config_file:
        parser.error("datahub's config file has not been specified!")
    return options
