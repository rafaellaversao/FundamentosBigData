/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mycompany.implementacaomapreduce;

// Imports.
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author Rafaella
 */

// País com a maior quantidade de transações comerciais efetuadas.
public class Informacao5 {

    public static class MapperInformacao5 extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable ocorrencia = new IntWritable(1);
        private final Text chaveMap = new Text();

        @Override
        public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException {
            String linha = valor.toString();
            String[] campos = linha.split(";");
            if (campos.length == 10) {
                String pais = campos[0].trim();
                chaveMap.set(pais);
                context.write(chaveMap, ocorrencia);
            }
        }
    }

    public static class ReducerInformacao5 extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final Text paisMax = new Text();
        private final IntWritable maxOcorrencia = new IntWritable(0);

        @Override
        public void reduce(Text chave, Iterable<IntWritable> valores, Context context) throws IOException, InterruptedException {
            int soma = 0;
            for (IntWritable val : valores) {
                soma += val.get();
            }

            if (soma > maxOcorrencia.get()) {
                maxOcorrencia.set(soma);
                paisMax.set(chave);
            }

            context.write(chave, new IntWritable(soma));
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("País com a maior quantidade de transações comerciais:"), maxOcorrencia);
            context.write(paisMax, maxOcorrencia);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String arquivoEntrada = "/Users/Rafaella/Documents/NetBeansProjects/Implementacao/src/main/java/com/pucpr/implementacao/base_100_mil.csv";
        String arquivoSaida = "/Users/Rafaella/Documents/NetBeansProjects/Implementacao/src/main/java/com/pucpr/implementacao/informacao5.java";
        if (args.length == 2) {
            arquivoEntrada = args[0];
            arquivoSaida = args[1];
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "informacao5");
        job.setJarByClass(Informacao5.class);
        job.setMapperClass(MapperInformacao5.class);
        job.setReducerClass(ReducerInformacao5.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
        FileOutputFormat.setOutputPath(job, new Path(arquivoSaida));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
