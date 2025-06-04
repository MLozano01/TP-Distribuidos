from multiprocessing import Process, Queue, Event
import logging
from common.transformer import Transformer
from protocol.utils.communicator import Communicator

logging.getLogger("pika").setLevel(logging.ERROR)


class Controller:
    def __init__(self, config, communication_config):
        self.transformer = None
        self.transformer_communicator = None
        self.config = config
        self.communication_config = communication_config
        self.stop_event = Event()

    def start(self):
        logging.info("Starting transformer")

        try:

            finish_notify_ntc = Queue()
            finish_notify_ctn = Queue()



            communicator_instance = Communicator(self.communication_config, finish_notify_ntc, finish_notify_ctn, self.stop_event)

            self.transformer_communicator = Process(target=communicator_instance.run, args=())
            self.transformer_communicator.start()
            
            transformer_instance = Transformer(finish_notify_ntc, finish_notify_ctn, communicator_instance, **self.config)

            self.transformer = Process(target=transformer_instance.start, args=())
        
            self.transformer.start()
            logging.info(f"Transformer communicator started with PID: {self.transformer_communicator.pid}")


            self.transformer.join()        
            self.transformer_communicator.join()

        except KeyboardInterrupt:
            logging.info("Transformer stopped by user")
        except Exception as e:
            logging.error(f"Transformer error: {e}")
        

    
    def stop(self):
        if self.transformer:
            self.transformer.terminate()
            self.transformer.join()
            logging.info("Transformer process terminated")
        else:
            logging.warning("No transformer process to terminate")
        self.transformer = None
    