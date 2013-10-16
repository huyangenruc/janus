package cn.bcc.hbase;

public class ArgTest {

    /**
     * @param args
     */
    public static void hint(){
        System.out.println("Usage : java -jar janus.jar --confPath= --receptorPath= --ligandPath= --seed= --topK= --vinaJobName= ");
        System.out.println("�����б�");
        System.out.println("--confPath="+" : "+"conf�����ļ�����Ŀ¼");
        System.out.println("--receptorPath="+" : "+"receptor�ļ�����Ŀ¼");
        System.out.println("--ligandPath="+" : "+"ligand�ļ�����Ŀ¼");
        System.out.println("--seed="+" : "+"seedֵ");
        System.out.println("--topK="+" : "+"ɸѡtopK��");
        System.out.println("--vinaJobName="+" : "+"��ҵ��");
    }
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        String[] argument = new String[6];
        String[] name={"confPath","receptorPath","ligandPath","seed","topK","vinaJobID"};
        
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
        
       for(int i=0;i<argument.length;i++){
           if(null==argument[i]){
               System.out.println("����"+name[i]+"����");
               hint();
               System.exit(0);
           }else{
               int position=argument[i].lastIndexOf("=");
               if(position==argument[i].length()-1){
                   System.out.println("����"+name[i]+"����,��������Ϊ��");
                   hint();
                   System.exit(0); 
               }
              
               argument[i]=argument[i].substring(position+1, argument[i].length());
               if(i==4){
                   try{
                       int k=Integer.parseInt(argument[i]);
                       if(k<=0){
                           System.out.println("����"+name[i]+"����,topK����Ϊ������");
                           hint();
                           System.exit(0);
                       }
                   }catch(NumberFormatException e){
                       //e.printStackTrace();
                       System.out.println("����"+name[i]+"����,topK����Ϊ����");
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
       String topK = argument[4];
       String vinaJobID = argument[5];
       

       


        
    }

}
