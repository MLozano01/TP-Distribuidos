from multiprocessing import Process, Queue, Event
import logging
import signal
from common.joiner import Joiner
from protocol.utils.communicator import Communicator
logging.getLogger("pika").setLevel(logging.ERROR)


class Controller:
    def __init__(self, config, communication_config):
        self.joiner = None
        self.joiner_communicator = None
        self.config = config
        self.communication_config = communication_config
        self.comm_queue = []
        self.stop_event = Event()


    def start(self):
        logging.info("Starting Joiner")
        self._set_up_signal_handlers()

        try:
            joiner_name = self.config["joiner_name"]
            finish_notify_ntc = Queue()
            finish_notify_ctn = Queue()
            self.comm_queue = [finish_notify_ntc, finish_notify_ctn]

            logging.info(f"Starting joiner {joiner_name}  with config {self.config}")

            communicator_instance = Communicator(self.communication_config, finish_notify_ntc, finish_notify_ctn, self.stop_event)
            self.joiner_communicator = Process(target=communicator_instance.run, args=())
            self.joiner_communicator.start()
            logging.info(f"Joiner communicator started with PID: {self.joiner_communicator.pid}")

            joiner_instance = Joiner( finish_notify_ntc, finish_notify_ctn, self.stop_event, communicator_instance, **self.config)
            self.joiner = Process(target=joiner_instance.run, args=())
        
            self.joiner.start()
            logging.info(f"Joiner {joiner_name} started with PID: {self.joiner.pid}")


            self.joiner.join()        
            logging.info("Joiner process terminated")
            self.joiner_communicator.join()
            logging.info("Joiner communicator process terminated")

        except KeyboardInterrupt:
            logging.info("Joiner stopped by user")
            self.stop()
        except Exception as e:
            logging.error(f"Joiner error: {e}")
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