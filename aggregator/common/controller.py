from multiprocessing import Process
import logging
from aggregator.common.aggregator import Aggregator
import common.config_init


class Controller:
    def __init__(self, config):
        self.all_aggregators = []
        self.config = config

    def start(self):
        logging.info("Starting aggrs")

        for i in range(self.config["aggr_num"]):

            aggr_config = self.config[f"aggr_{i}"]

            aggr_path = aggr_config.split(",")[0]

            aggr_config_params = common.config_init.config_aggr(f'/{aggr_path}.ini')

            try:
                logging.info(f"Starting aggr {i} with config: {aggr_config_params}")

                aggr_instance = Aggregator(**aggr_config_params)

                new_aggr = Process(target=aggr_instance.run, args=())
                self.all_aggregators.append(new_aggr)

                new_aggr.start()
                logging.info(f"Aggr {i} started with PID: {new_aggr.pid}")

            except KeyboardInterrupt:
                logging.info("Aggr stopped by user")
                self.stop()
            except Exception as e:
                logging.error(f"Aggr error: {e}")
                self.stop()
            finally:
                for aggr in self.all_aggregators:
                    aggr.join()
    
    def stop(self):
        for aggr in self.all_aggregators:
            if aggr.is_alive():
                aggr.terminate()