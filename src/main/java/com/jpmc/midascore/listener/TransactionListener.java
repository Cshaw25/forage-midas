package com.jpmc.midascore.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;

import jakarta.transaction.Transactional;

@Service
public class TransactionListener {

    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;
    private final ObjectMapper mapper = new ObjectMapper();


    public TransactionListener(UserRepository userRepository,TransactionRepository transactionRepository) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
    }

    @Transactional
    @KafkaListener(id = "myId", topics = "${general.kafka-topic}")
    public void listen(String message) throws Exception {
        
        Transaction x = mapper.readValue(message, Transaction.class);   // converts JSON to Transaction object
        UserRecord sender = userRepository.findById(x.getSenderId());   
        UserRecord recipient = userRepository.findById(x.getRecipientId());

        // checks if transaction is valid 
        if ((sender == null) || (recipient == null) || (x.getAmount() > sender.getBalance())){
            return;
        }

        // Send Post request to incetive API to get an incentive object. (holds how much incentive) 

        RestTemplate restTemplate = new RestTemplate();
        Incentive incentive = restTemplate.postForObject(
        "http://localhost:8080/incentive",
            x, Incentive.class
        );

        

        // start saving everything after the transaction is valid 

        TransactionRecord tr = new TransactionRecord(sender, recipient, x.getAmount(),incentive.getAmount());
        transactionRepository.save(tr);

        sender.setBalance(sender.getBalance() - x.getAmount());
        recipient.setBalance(recipient.getBalance() + x.getAmount() + incentive.getAmount());
        userRepository.save(sender);
        userRepository.save(recipient);

        

        System.out.println(String.format(
    "Transaction of %.2f from %s to %s successful! Sender balance: %.2f, Recipient balance: %.2f" + " Incentive: " + incentive.getAmount(),
    x.getAmount(),
    sender.getName(),
    recipient.getName(),
    sender.getBalance(),
    recipient.getBalance()
));

    }
}
