package cn.bcc.core;

public class QuickStart {

    public static void hint() {
        System.out
                .println("Usage : java -jar janus.jar --confPath= --receptorPath= --ligandPath= --seed= --topK= --vinaJobName= ");
        System.out.println("参数列表：");
        System.out.println("--confPath=" + " : " + "conf配置文件本地目录");
        System.out.println("--receptorPath=" + " : " + "receptor文件本地目录");
        System.out.println("--ligandPath=" + " : " + "ligand文件本地目录");
        System.out.println("--seed=" + " : " + "seed值");
        System.out.println("--topK=" + " : " + "筛选topK数");
        System.out.println("--vinaJobName=" + " : " + "作业名");
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

        String[] argument = new String[6];
        String[] name = {"confPath", "receptorPath", "ligandPath", "seed", "topK", "vinaJobID"};

        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--confPath=")) {
                argument[0] = args[i];
            } else if (args[i].startsWith("--receptorPath=")) {
                argument[1] = args[i];
            } else if (args[i].startsWith("--ligandPath=")) {
                argument[2] = args[i];
            } else if (args[i].startsWith("--seed=")) {
                argument[3] = args[i];
            } else if (args[i].startsWith("--topK=")) {
                argument[4] = args[i];
            } else if (args[i].startsWith("--vinaJobName=")) {
                argument[5] = args[i];
            }
        }

        for (int i = 0; i < argument.length; i++) {
            if (null == argument[i]) {
                System.out.println("请检查" + name[i] + "参数");
                hint();
                System.exit(0);
            } else {
                int position = argument[i].lastIndexOf("=");
                if (position == argument[i].length() - 1) {
                    System.out.println("请检查" + name[i] + "参数,参数不能为空");
                    hint();
                    System.exit(0);
                }

                argument[i] = argument[i].substring(position + 1, argument[i].length());
                if (i == 4) {
                    try {
                        int k = Integer.parseInt(argument[i]);
                        if (k <= 0) {
                            System.out.println("请检查" + name[i] + "参数,topK必须为正整数");
                            hint();
                            System.exit(0);
                        }
                    } catch (NumberFormatException e) {
                        // e.printStackTrace();
                        System.out.println("请检查" + name[i] + "参数,topK必须为整数");
                        hint();
                        System.exit(0);
                    }

                }
            }
        }

        String confPath = argument[0];
        String receptorPath = argument[1];
        String ligandPath = argument[2];
        String seed = argument[3];
        int topK = Integer.parseInt(argument[4]);
        String vinaJobID = argument[5];
        try {

            VinaHadoop job = new VinaHadoop();
            job.startJob(confPath, receptorPath, ligandPath, seed, topK, vinaJobID, 100, true);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

}
