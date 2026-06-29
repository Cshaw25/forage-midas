package com.jpmc.midascore.controller;

import com.jpmc.midascore.foundation.Balance;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BalanceController {

    private final UserRepository userRepository;

    public BalanceController(UserRepository userRepository) {
        this.userRepository = userRepository;
    }
    @GetMapping("/balance")
    public Balance balance(@RequestParam("userId") long id){
        UserRecord client = userRepository.findById(id);

        if (client==null){
            Balance x = new Balance(0);
            return x;
        }
        
        Balance x = new Balance(client.getBalance());
        return x;
    }
}
