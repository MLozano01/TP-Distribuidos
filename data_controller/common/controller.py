from multiprocessing import Process, Queue
import logging
import signal
import os
import threading
from common.data_controller import DataController
from common.data_controller_communicator import DataControllerCommunicator

logging.getLogger("pika").setLevel(logging.ERROR)


class Controller:
    def __init__(self, config, movies_communication_config, credits_communication_config, ratings_communication_config):
        self.data_controller = None
        self.data_controller_communicator = None
        self.config = config
        self.movies_communication_config = movies_communication_config
        self.credits_communication_config = credits_communication_config
        self.ratings_communication_config = ratings_communication_config

    def start(self):
        logging.info("Starting DataController")
        self._setup_signal_handlers()

        try:
            movie_comm_queue = Queue()
            credits_comm_queue = Queue()
            ratings_comm_queue = Queue()
            data_controller_instance = DataController(movie_comm_queue, credits_comm_queue, ratings_comm_queue, **self.config)

            self.data_controller = Process(target=data_controller_instance.run, args=())
            self.data_controller.start()
            logging.info(f"DataController started with PID: {self.data_controller.pid}")

            movies_communicator_instance = DataControllerCommunicator(self.movies_communication_config, movie_comm_queue, "MOVIES")
            credits_communicator_instance = DataControllerCommunicator(self.credits_communication_config, credits_comm_queue, "CREDITS")
            ratings_communicator_instance = DataControllerCommunicator(self.ratings_communication_config, ratings_comm_queue, "RATINGS")

            self.movies_data_controller_communicator = Process(target=movies_communicator_instance.run, args=())
            self.movies_data_controller_communicator.start()

            self.credits_data_controller_communicator = Process(target=credits_communicator_instance.run, args=())
            self.credits_data_controller_communicator.start()

            self.ratings_data_controller_communicator = Process(target=ratings_communicator_instance.run, args=())
            self.ratings_data_controller_communicator.start()


            self.data_controller.join()        
            self.movies_data_controller_communicator.join()
            self.credits_data_controller_communicator.join()
            self.ratings_data_controller_communicator.join()

        except KeyboardInterrupt:
            logging.info("DataController stopped by user")
        except Exception as e:
            logging.error(f"DataController error: {e}")


    def _setup_signal_handlers(self):
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)
        

    def _handle_shutdown(self, signum, frame):
        logging.info(f"Received signal {signum}. Shutting down gracefully...")
        self.stop()
        logging.info("Shutdown complete.")

    
    def stop(self): # TODO COMPLETE
        if self.data_controller:
            self.data_controller.terminate()
            self.data_controller.join()
            logging.info("DataController process terminated")
        else:
            logging.warning("No DataController process to terminate")
        self.data_controller = None
    