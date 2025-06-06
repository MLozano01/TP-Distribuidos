from multiprocessing import Process, Queue, Event
import logging
import signal
import os
import threading
from common.data_controller import DataController
from protocol.utils.communicator import Communicator


logging.getLogger("pika").setLevel(logging.ERROR)


class Controller:
    def __init__(self, config, communication_config):
        self.data_controller = None
        self.data_controller_communicator = None
        self.config = config
        self.communication_config = communication_config
        self.queues = []
        self.stop_event = Event()
        
    def start(self):
        logging.info("Starting DataController")
        self._setup_signal_handlers()

        try:
            # Movies communication
            finish_notify_ntc = Queue()
            finish_notify_ctn = Queue()
            self.queues.append(finish_notify_ntc)
            self.queues.append(finish_notify_ctn)


            communicator_instance = Communicator(self.communication_config, finish_notify_ntc, finish_notify_ctn, self.stop_event)
            
            self.data_controller_communicator = Process(target=communicator_instance.run, args=())
            self.data_controller_communicator.start()
            data_controller_instance = DataController(finish_notify_ntc, finish_notify_ctn, communicator_instance, **self.config)

            self.data_controller = Process(target=data_controller_instance.run, args=())
            self.data_controller.start()
            logging.info(f"DataController started with PID: {self.data_controller.pid}")



            self.data_controller.join()   
            self.data_controller_communicator.join()

        except KeyboardInterrupt:
            logging.info("DataController stopped by user")
        except Exception as e:
            logging.error(f"DataController error: {e}")


    def _setup_signal_handlers(self):
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)
        

    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals"""
        logging.info(f"Received signal {signum}. Shutting down gracefully...")
        try:

            for queue in self.queues:
                queue.close()
            for queue in self.queues:
                queue.join_thread()
            self.queues = []  
            # First stop the data controller which will close its connections
            if self.data_controller:
                logging.info("Stopping DataController process...")
                self.data_controller.terminate()

            if self.data_controller_communicator:
                logging.info(f"Stopping communicator process...")
                self.data_controller_communicator.terminate()
            
            self.data_controller.join() 
            logging.info("DataController process stopped")
            
            if self.data_controller_communicator:
                self.data_controller_communicator.join()
                logging.info(f"Communicator process stopped")
            
            logging.info("All processes stopped successfully")
        except Exception as e:
            logging.error(f"Error during shutdown: {e}")
        finally:
            logging.info("Shutdown complete.")

    def stop(self):
        """Stop all processes gracefully"""
        self._handle_shutdown(signal.SIGTERM, None)
    