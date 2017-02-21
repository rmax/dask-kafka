import logging

from kafka_scanner import KafkaScannerDirect as _KafkaScannerDirect


logger = logging.getLogger(__name__)


class KafkaScannerDirect(_KafkaScannerDirect):

    def get_new_batch(self):
        batchsize = self._KafkaScanner__batchsize
        if self._count > 0:
            batchsize = min(self._KafkaScanner__batchsize, self._count - self._KafkaScanner__issued_count)

        # Skip message processing.
        self._init_batch(batchsize)
        return self._scan_topic_batch(batchsize)

    def scan_topic_batches(self):
        self.init_scanner()
        records = []
        while self.enabled:
            if not self.are_there_messages_to_process():
                break
            for batch in self.get_new_batch():
                if (
                    self._KafkaScanner__batchcount > 0
                    and self._KafkaScanner__issued_batches == self._KafkaScanner__batchcount - 1
                ):
                    self.enabled = False
                for message in batch:
                    self._KafkaScanner__last_message[message['partition']] = message['offset']
                    records.append(message)
                    if len(records) == self._KafkaScanner__batchsize:
                        if self._KafkaScanner__batch_autocommit:
                            self.end_batch_commit()
                        yield records
                        records = []
                        self._KafkaScanner__issued_batches += 1
        if records:
            yield records
            self._KafkaScanner__issued_batches += 1

        self.end_batch_commit()

        self.stats_logger.log_stats(totals=True)

        logger.info("Total batches Issued: %d", self._KafkaScanner__issued_batches)
        scan_efficiency = 100.0 - (
            100.0 * (
                self._KafkaScanner__real_scanned_count - self.scanned_count
            ) / self._KafkaScanner__real_scanned_count
        ) if self._KafkaScanner__real_scanned_count else 100.0
        logger.info("Real Scanned: {}".format(self._KafkaScanner__real_scanned_count))
        logger.info("Scan efficiency: {:.2f}%".format(scan_efficiency))

        self.close()
