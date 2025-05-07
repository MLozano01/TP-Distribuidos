from multiprocessing import Process, Queue
import logging
from common.filter import Filter
from common.filter_communicator import FilterCommunicator

logging.getLogger("pika").setLevel(logging.ERROR)


class Controller:
    def __init__(self, config, communication_config):
        self.filter = None
        self.filter_communicator = None
        self.config = config
        self.communication_config = communication_config

    def start(self):
        logging.info("Starting Filters")

        try:

            finish_receive_ntc = Queue()
            finish_notify_ntc = Queue()
            finish_receive_ctn = Queue()
            finish_notify_ctn = Queue()

            filter_name = self.config["filter_name"]

            logging.info(f"Starting filter {filter_name}  with config {self.config}")

            filter_instance = Filter(finish_receive_ntc, finish_notify_ntc, finish_receive_ctn, finish_notify_ctn, **self.config)

            self.filter = Process(target=filter_instance.run, args=())
        
            self.filter.start()
            logging.info(f"Filter {filter_name} started with PID: {self.filter.pid}")

            communicator_instance = FilterCommunicator(self.communication_config, finish_receive_ntc, finish_notify_ntc, finish_receive_ctn, finish_notify_ctn)
            self.filter_communicator = Process(target=communicator_instance.run, args=())
            self.filter_communicator.start()
            logging.info(f"Filter communicator started with PID: {self.filter_communicator.pid}")


            self.filter.join()        
            self.filter_communicator.join()

        except KeyboardInterrupt:
            logging.info("Filter stopped by user")
        except Exception as e:
            logging.error(f"Filter error: {e}")
        

    
    def stop(self):
        if self.filter:
            self.filter.terminate()
            self.filter.join()
            logging.info("Filter process terminated")
        else:
            logging.warning("No filter process to terminate")
        self.filter = None
    