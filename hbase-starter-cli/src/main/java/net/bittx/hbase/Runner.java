package net.bittx.hbase;



import net.bittx.hbase.service.StarterService;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

@Component
public class Runner implements ApplicationRunner {

    @Resource
    StarterService starterService;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        System.out.println(">>>>>>>>>>>>>>>>...");
        System.out.println(starterService.get());
        starterService.save();

/*
        StarterService.UserInfo ui = starterService.getUserInfo();

        System.out.println(ui.getMem());
        System.out.println(ui.getName());*/

        List<StarterService.UserInfo> ul = starterService.listUserInfo();
        System.out.println("UUUUUUUUUUUUUUUUU");
        ul.forEach(o->{
            System.out.println(o.getName() + "   " + o.getMem());
        });

    }
}
