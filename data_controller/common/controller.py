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
            # Movies communication
            movies_finish_receive_ntc = Queue()
            movies_finish_notify_ntc = Queue()
            movies_finish_receive_ctn = Queue()
            movies_finish_notify_ctn = Queue()

            # Credits communication
            credits_finish_receive_ntc = Queue()
            credits_finish_notify_ntc = Queue()
            credits_finish_receive_ctn = Queue()
            credits_finish_notify_ctn = Queue()

            # Ratings communication
            ratings_finish_receive_ntc = Queue()
            ratings_finish_notify_ntc = Queue()
            ratings_finish_receive_ctn = Queue()
            ratings_finish_notify_ctn = Queue()

            data_controller_instance = DataController(movies_finish_receive_ntc, movies_finish_notify_ntc, movies_finish_receive_ctn, movies_finish_notify_ctn, credits_finish_receive_ntc, credits_finish_notify_ntc, credits_finish_receive_ctn, credits_finish_notify_ctn, ratings_finish_receive_ntc, ratings_finish_notify_ntc, ratings_finish_receive_ctn, ratings_finish_notify_ctn, **self.config)

            self.data_controller = Process(target=data_controller_instance.run, args=())
            self.data_controller.start()
            logging.info(f"DataController started with PID: {self.data_controller.pid}")

            movies_communicator_instance = DataControllerCommunicator(self.movies_communication_config, movies_finish_receive_ntc, movies_finish_notify_ntc, movies_finish_receive_ctn, movies_finish_notify_ctn, "MOVIES")
            credits_communicator_instance = DataControllerCommunicator(self.credits_communication_config, credits_finish_receive_ntc, credits_finish_notify_ntc, credits_finish_receive_ctn, credits_finish_notify_ctn, "CREDITS")
            ratings_communicator_instance = DataControllerCommunicator(self.ratings_communication_config, ratings_finish_receive_ntc, ratings_finish_notify_ntc, ratings_finish_receive_ctn, ratings_finish_notify_ctn, "RATINGS")

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
        """Handle shutdown signals"""
        logging.info(f"Received signal {signum}. Shutting down gracefully...")
        try:
            # First stop the data controller which will close its connections
            if self.data_controller:
                logging.info("Stopping DataController process...")
                self.data_controller.terminate()
                try:
                    self.data_controller.join(timeout=5)  # Wait up to 5 seconds
                    if self.data_controller.is_alive():
                        logging.warning("DataController did not terminate gracefully, forcing...")
                        self.data_controller.kill()
                except Exception as e:
                    logging.error(f"Error waiting for DataController to terminate: {e}")
                logging.info("DataController process stopped")
            
            # Then stop all communicators
            communicators = [
                (self.movies_data_controller_communicator, "Movies"),
                (self.credits_data_controller_communicator, "Credits"),
                (self.ratings_data_controller_communicator, "Ratings")
            ]
            
            for comm, name in communicators:
                if comm:
                    logging.info(f"Stopping {name} communicator process...")
                    comm.terminate()
                    try:
                        comm.join(timeout=5)  # Wait up to 5 seconds
                        if comm.is_alive():
                            logging.warning(f"{name} communicator did not terminate gracefully, forcing...")
                            comm.kill()
                    except Exception as e:
                        logging.error(f"Error waiting for {name} communicator to terminate: {e}")
                    logging.info(f"{name} communicator process stopped")
            
            # Clear references
            self.data_controller = None
            self.movies_data_controller_communicator = None
            self.credits_data_controller_communicator = None
            self.ratings_data_controller_communicator = None
            
            logging.info("All processes stopped successfully")
        except Exception as e:
            logging.error(f"Error during shutdown: {e}")
        finally:
            logging.info("Shutdown complete.")
            os._exit(0)  # Force exit after cleanup

    def stop(self):
        """Stop all processes gracefully"""
        self._handle_shutdown(signal.SIGTERM, None)
    