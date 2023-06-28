/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.pucpr.implementacao;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.text.NumberFormat;
import java.util.Locale;
import org.apache.hadoop.fs.FileSystem;


/**
 *
 * @author Rafaella
 */
public class Informacao7 {
    
    static long maiorPeso = 0;
    static String maiorMercadoria;
    
    public static class MapperInformacao7 extends Mapper<Object, Text, Text, IntWritable>{
        
        @Override
        public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException{
            String linha = valor.toString();
            String[] campos = linha.split(";");
            IntWritable valorMap = new IntWritable(0);
            if (campos.length == 10){
                String mercadoria = campos[3];
                String peso = campos[6];
                
                try{
                    valorMap = new IntWritable(Integer.parseInt(peso));
                }catch(NumberFormatException e){
                    
                }finally{
                    
                }
                    
                Text chaveMap = new Text(mercadoria);
                context.write(chaveMap, valorMap);
            }
        }
        
    }
    
    public static class ReducerInformacao7 extends Reducer<Text, IntWritable, Text, LongWritable>{
        
        @Override
        public void reduce(Text chave, Iterable<IntWritable> valores, Context context) throws IOException, InterruptedException{
            long soma = 0;
            for (IntWritable val: valores){
                soma += val.get();
            }
            LongWritable valorSaida = new LongWritable(soma);
            context.write(chave, valorSaida);
            if(soma > maiorPeso){
                maiorPeso = soma;
                maiorMercadoria = chave.toString();
                
            }
            
          
        }
    }
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String arquivoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_100_mil.csv";
//        String arquivoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_inteira.csv";

        String arquivoSaida = "/home2/ead2022/SEM1/marcos.alarcon/Desktop/ATP/informacao7";

        if(args.length == 2){
            arquivoEntrada = args[0];
            arquivoSaida = args[1];
        }

        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "informacao7");
        
        job.setJarByClass(Informacao7.class);
        job.setMapperClass(MapperInformacao7.class);
        job.setReducerClass(ReducerInformacao7.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
        FileOutputFormat.setOutputPath(job, new Path(arquivoSaida));
        
        //Exclui arquivo de saída se existir
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(new Path(arquivoSaida)))
            hdfs.delete(new Path(arquivoSaida), true);

        job.waitForCompletion(true);
        
        //carrega idioma 
        Locale meuLocal = new Locale( "pt", "BR" );
        
        System.out.print("A Mercadoria com maior peso entre todas as transações comerciais é: "+maiorMercadoria+ ", somando um peso total de "+NumberFormat.getNumberInstance(meuLocal).format(maiorPeso)+" toneladas");

      
        
    }
    
}
    
