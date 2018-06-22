# -*- coding: utf-8 -*- 
from pyspark import SparkConf, SparkContext 
import sys
print("The Python version is %s.%s.%s" % sys.version_info[:3])
#import sys   
reload(sys) 
sys.setdefaultencoding('utf-8') 
  
conf = SparkConf().setMaster("local").setAppName("My App")  
sc = SparkContext(conf = conf)

testRDD = sc.textFile('datas/test.dne')
wordRDD = testRDD.flatMap(lambda line: line.split()).distinct()
#for w in wordRDD.collect():
#    w = w.encode('utf-8')
#    print("Word='{}'".format(w))

#print("first line={}".format(testRDD.first().encode('utf-8'))) 

if len(sys.argv) > 1:
    target_sent = sys.argv[1].encode('utf-8')
else:
    target_sent = '今天還是要去公司'.encode('utf-8')
#print('target_sent={}'.format(target_sent))

def tokenCheck(token_line):
    #print("token line={} ({})".format(token_line.encode('utf-8'), len(token_line)))
    global target_sent
    sent = target_sent
    token_list = token_line.split()
    result_token_list = []
    mc = 0  # Missing count
    hc = 0  # Hit count
    for token in token_list:
        token = token.encode('utf-8')
        token_len = len(token)
        try:
            pi = sent.index(token)
            hc += 1
            #print('\tToken={}({}) is found...(pi={})'.format(token, token_len, pi))
            if pi > 0:                
                #print('Add missing {}'.format(sent[0:pi]))
                result_token_list.append("*{}".format(sent[0:pi]))
                result_token_list.append("+{}".format(token))
                sent = sent[pi+token_len:]
            else:
                result_token_list.append("+{}".format(token))
                sent = sent[pi+token_len:]

        except ValueError:
            #print('\tToken={} is not found!'.format(token))
            mc += 1            
        except:
            raise

        if len(sent) == 0:
            break
            

    if len(sent) > 0:
        result_token_list.append("*{}".format(sent))


    #print("{}...mc={}/hc={}: {}".format(token_line.encode('utf-8'), mc, hc, ' '.join(result_token_list)))
    #return "{} {} {}".format(hc, mc, ' '.join(result_token_list))
    return ((' '.join(result_token_list), token_line), hc-0.1*mc)
    #return mc <= 2 and hc > 0

def tokenize(sub_line):
    def _map2Tokenize(token_line):
        token_line = token_line.encode('utf-8')
        sent = sub_line
        #print('sub_line={}'.format(sent))        
        token_list = token_line.split()
        result_token_list = []
        mc = 0  # Missing count
        hc = 0  # Hit count
        for token in token_list:            
            token_len = len(token)
            try:
                pi = sent.index(token)
                hc += 1
                if pi > 0:
                    result_token_list.append("*{}".format(sent[0:pi]))
                    result_token_list.append(token)
                    sent = sent[pi+token_len:]
                else:
                    result_token_list.append(token)
                    sent = sent[pi+token_len:]

            except ValueError:
                mc += 1
            except:
                raise

            if len(sent) == 0:
                break


        if len(sent) > 0:
            result_token_list.append("*{}".format(sent))

        return ((' '.join(result_token_list), token_line), hc-0.1*mc)

    rstRDD = testRDD.map(_map2Tokenize).reduceByKey(lambda a, b: a + b).filter(lambda e: e[1] > 0)
    final_token_list = []
    for t, v in sorted(rstRDD.collect(), key=lambda e: e[1], reverse=True):
        k = t[0]
        if ' ' not in k:
            break
        #print('\tCheck {}'.format(k))
        for token in k.split():            
            if token.startswith('*'):
                ctoken = token[1:]
                if wordRDD.filter(lambda e: e.encode('utf-8')==ctoken).count() == 0:
                    final_token_list = []
                    break
                else:                   
                    final_token_list.append("-{}".format(ctoken))
            else:
                final_token_list.append("-{}".format(token))
        break

    return final_token_list


trtRDD = testRDD.map(tokenCheck).reduceByKey(lambda a, b: a + b).filter(lambda e: e[1] > 0)
print("Candidate number={}...".format(trtRDD.count()))
for t, v in sorted(trtRDD.collect(), key=lambda e: e[1], reverse=True):
    k = t[0].encode('utf-8')
    final_token_list = []    
    for token in k.split():
        dtoken = token.encode('utf-8')
        if dtoken.startswith('*'):
            ctoken = dtoken[1:]
            if wordRDD.filter(lambda e: e.encode('utf-8')==ctoken).count() == 0:                
                if (len(ctoken)/3) > 4:
                    sub_token_list = tokenize(dtoken[1:])
                    #print('\tRetokenize {} ({}): \'{}\''.format(ctoken, len(ctoken), ' '.join(sub_token_list)))
                    if sub_token_list:
                        #print('\t{}->{}'.format(token, sub_token_list))
                        final_token_list.extend(sub_token_list)
                        continue

                final_token_list.append(token)
            else:
                final_token_list.append(ctoken)
        else:
            final_token_list.append(token)


    print("Candidate Tokenized Result={} (v={} by '{}')".format(" ".join(final_token_list), v, t[1].encode('utf-8')))            
