from multiprocessing import Process, Queue, Event
import logging
import signal
from common.filter import Filter
from common.filter_communicator import FilterCommunicator

logging.getLogger("pika").setLevel(logging.ERROR)


class Controller:
    def __init__(self, config, communication_config):
        self.filter = None
        self.filter_communicator = None
        self.config = config
        self.communication_config = communication_config
        self.comm_queue = []
        self.stop_event = Event()


    def start(self):
        logging.info("Starting Filters")
        self._set_up_signal_handlers()

        try:
            filter_name = self.config["filter_name"]
            finish_receive_ntc = Queue()
            finish_notify_ntc = Queue()
            finish_receive_ctn = Queue()
            finish_notify_ctn = Queue()
            self.comm_queue = [finish_receive_ntc, finish_notify_ntc, finish_receive_ctn, finish_notify_ctn]

            logging.info(f"Starting filter {filter_name}  with config {self.config}")

            filter_instance = Filter(finish_receive_ntc, finish_notify_ntc, finish_receive_ctn, finish_notify_ctn, self.stop_event, **self.config)
            self.filter = Process(target=filter_instance.run, args=())
        
            self.filter.start()
            logging.info(f"Filter {filter_name} started with PID: {self.filter.pid}")

            communicator_instance = FilterCommunicator(self.communication_config, finish_receive_ntc, finish_notify_ntc, finish_receive_ctn, finish_notify_ctn, self.stop_event)
            self.filter_communicator = Process(target=communicator_instance.run, args=())
            self.filter_communicator.start()
            logging.info(f"Filter communicator started with PID: {self.filter_communicator.pid}")


            self.filter.join()        
            logging.info("Filter process terminated")
            self.filter_communicator.join()
            logging.info("Filter communicator process terminated")

        except KeyboardInterrupt:
            logging.info("Filter stopped by user")
            self.stop()
        except Exception as e:
            logging.error(f"Filter error: {e}")
            self.stop()
        

    def stop(self):
        self.stop_event.set()
        for queue in self.comm_queue:
            if queue:
                queue.put("STOP_EVENT")
        for queue in self.comm_queue:
            if queue:
                queue.close()

    
    def _set_up_signal_handlers(self):
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum, frame):
        logging.info(f"Received signal {signum}, stopping...")
        self.stop()