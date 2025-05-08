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
        self.queues = []
        
    def start(self):
        logging.info("Starting DataController")
        self._setup_signal_handlers()

        try:
            # Movies communication
            movies_finish_receive_ntc = Queue()
            movies_finish_notify_ntc = Queue()
            movies_finish_receive_ctn = Queue()
            movies_finish_notify_ctn = Queue()
            self.queues.append(movies_finish_receive_ntc)
            self.queues.append(movies_finish_notify_ntc)
            self.queues.append(movies_finish_receive_ctn)
            self.queues.append(movies_finish_notify_ctn)

            # Credits communication
            credits_finish_receive_ntc = Queue()
            credits_finish_notify_ntc = Queue()
            credits_finish_receive_ctn = Queue()
            credits_finish_notify_ctn = Queue()

            self.queues.append(credits_finish_receive_ntc)
            self.queues.append(credits_finish_notify_ntc)
            self.queues.append(credits_finish_receive_ctn)
            self.queues.append(credits_finish_notify_ctn)
            # Ratings communication
            ratings_finish_receive_ntc = Queue()
            ratings_finish_notify_ntc = Queue()
            ratings_finish_receive_ctn = Queue()
            ratings_finish_notify_ctn = Queue()

            self.queues.append(ratings_finish_receive_ntc)
            self.queues.append(ratings_finish_notify_ntc)
            self.queues.append(ratings_finish_receive_ctn)
            self.queues.append(ratings_finish_notify_ctn)

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

            for queue in self.queues:
                queue.close()
            for queue in self.queues:
                queue.join_thread()
            self.queues = []  
            # First stop the data controller which will close its connections
            if self.data_controller:
                logging.info("Stopping DataController process...")
                self.data_controller.terminate()

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
            
            self.data_controller.join() 
            logging.info("DataController process stopped")
            for comm, name in communicators:
                comm.join()
                logging.info(f"{name} communicator process stopped")
            
            logging.info("All processes stopped successfully")
        except Exception as e:
            logging.error(f"Error during shutdown: {e}")
        finally:
            logging.info("Shutdown complete.")

    def stop(self):
        """Stop all processes gracefully"""
        self._handle_shutdown(signal.SIGTERM, None)
    