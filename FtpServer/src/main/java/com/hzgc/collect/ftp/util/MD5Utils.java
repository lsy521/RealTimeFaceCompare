package com.hzgc.collect.ftp.util;

import com.hzgc.collect.ftp.util.EncryptUtils;

public class MD5Utils {
    public static void main(String args[]) {
        if (args.length == 0) {
            System.out.println("Parameter can not be empty!");
            System.exit(1);
        }
        if (args.length > 1) {
            System.out.println("Parameter can only be one!");
            System.exit(1);
        }
        if (args[0].length() > 0) {
            String passed = EncryptUtils.encryptMD5(args[0]);
            System.out.println("The password is:" + passed);
        } else {
            System.out.println("The parameter length must be greater than 0!");
            System.exit(1);
        }
    }
}
