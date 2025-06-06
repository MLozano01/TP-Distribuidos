from multiprocessing import Process, Queue, Event
import logging
from common.aggregator import Aggregator
from protocol.utils.communicator import Communicator

logging.getLogger("pika").setLevel(logging.ERROR)


class Controller:
    def __init__(self, config, communication_config):
        self.aggregator = None
        self.aggregator_communicator = None
        self.config = config
        self.communication_config = communication_config
        self.stop_event = Event()

    def start(self):
        logging.info("Starting Aggregator")

        try:

            finish_notify_ntc = Queue()
            finish_notify_ctn = Queue()


            communicator_instance = Communicator(self.communication_config, finish_notify_ntc, finish_notify_ctn, self.stop_event)

            self.aggregator_communicator = Process(target=communicator_instance.run, args=())
            self.aggregator_communicator.start()

            aggregator_instance = Aggregator(finish_notify_ntc, finish_notify_ctn, communicator_instance, **self.config)

            self.aggregator = Process(target=aggregator_instance.run, args=())
        
            self.aggregator.start()
            logging.info(f"Aggregator communicator started with PID: {self.aggregator_communicator.pid}")


            self.aggregator.join()        
            self.aggregator_communicator.join()

        except KeyboardInterrupt:
            logging.info("Aggregator stopped by user")
        except Exception as e:
            logging.error(f"Aggregator error: {e}")
        

    
    def stop(self):
        if self.aggregator:
            self.aggregator.terminate()
            logging.info("Aggregator process terminated")
        else:
            logging.warning("No aggregator process to terminate")
        self.aggregator = None
    