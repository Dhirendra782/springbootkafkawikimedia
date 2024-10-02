package com.springbootkafka.kafka;

import com.springbootkafka.entity.WikimediaData;
import com.springbootkafka.repository.WikimediaDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaDatabaseConsumer {
    public static final Logger logger = LoggerFactory.getLogger(KafkaDatabaseConsumer.class);

    private WikimediaDataRepository wikimediaDataRepository;
    public KafkaDatabaseConsumer(WikimediaDataRepository wikimediaDataRepository) {
        this.wikimediaDataRepository = wikimediaDataRepository;
    }

    @KafkaListener(topics = "${spring.jpa.hibernate.ddl-auto}",groupId = "${spring.kafka.consumer.group-id}")
    public void consume(String eventMessage) {
        logger.info(String.format("Event message received -> %s",eventMessage));
        WikimediaData wikimediaData = new WikimediaData();
        wikimediaData.setWikiEventDate(eventMessage);
        wikimediaDataRepository.save(wikimediaData);

    }

}












