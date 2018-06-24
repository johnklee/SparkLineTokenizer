# -*- coding: utf-8 -*- 
from pyspark import SparkConf, SparkContext 
import sys
print("The Python version is %s.%s.%s" % sys.version_info[:3])
reload(sys) 
sys.setdefaultencoding('utf-8') 
  

########################
# Global Variables
########################
conf = SparkConf().setMaster("local").setAppName("My App")  
sc = SparkContext(conf = conf)
testRDD = sc.textFile('datas/test.dne')
wordRDD = testRDD.flatMap(lambda line: line.split()).distinct()

topN = 10  # Show at most top 10 candidates

if len(sys.argv) > 1:
    target_sent = sys.argv[1].encode('utf-8')
    if len(sys.argv) > 2:
        topN = int(sys.argv[2])
else:
    target_sent = '今天還是要去公司'.encode('utf-8')


########################
# APIs
########################
def look4Coll(ptoken, etoken):
    r'''
    Look for collocaton from previous token as <ptoken> and 
    current error token as <etoken>
    '''
    if ptoken:
        if ptoken.startswith('+') or ptoken.startswith('-'):
            ptoken = ptoken[1:]
    
    if ptoken:
        print("Look for collocation: '{} {}'...".format(ptoken, etoken))
    else:
        print("Look for collocation: '^{}'...".format(etoken))

    def filterPtoken(token_line):
        token_list = token_line.split()
        encode_token_list = map(lambda t: t.encode('utf-8'), token_list)
        return ptoken in encode_token_list
    
    def collCheck(token_line):
        token_list = token_line.split()
        token_list_len = len(token_list)

        if ptoken is None:
            return ((None, token_list[0].encode('utf-8')), 1)
        else:
            for i in xrange(token_list_len):
                token = token_list[i].encode('utf-8')
                if token == ptoken and i+1 < token_list_len:
                    return ((token, token_list[i+1].encode('utf-8')), 1) 

    def b2s(sent):
        bs_list = []
        for i in range(len(sent)/3):
            bi = i*3
            bs_list.append(sent[bi:bi+3])

        return bs_list

    def sim_of_token(etoken, otoken):
        if len(etoken) != len(otoken):
            return False
 
        etoken_c_list = b2s(etoken)
        otoken_c_list = b2s(otoken)
        hc = 0
        for bs in etoken_c_list:
            if bs in otoken_c_list:
                hc += 1

        return float(hc) / len(otoken_c_list)

    def filterSimUp(e):
        t = e[0]
        s = e[1]
        return sim_of_token(etoken, t[1]) >= 0.5

    rstRDD = testRDD.filter(filterPtoken).map(collCheck).reduceByKey(lambda a, b: a + b).filter(filterSimUp)
    return rstRDD


def isTokenSeen(token):
    return wordRDD.filter(lambda e: e.encode('utf-8')==token).count() > 0


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
            #print('\tToken={}({}) is found...(pi={})'.format(token, token_len, pi))
            if pi > 0:                
                #print('Add missing {}'.format(sent[0:pi]))
		hc += 1	
                result_token_list.append("*{}".format(sent[0:pi]))
                result_token_list.append("+{}".format(token))
                sent = sent[pi+token_len:]
            else:
		hc += 2
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
                    ctoken = sent[0:pi]
                    #if isTokenSeen(ctoken):
                    #    hc += 0.5
                    #    result_token_list.append("~{}".format(ctoken))
                    #else:
                    result_token_list.append("*{}".format(ctoken))
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


########################
# MAIN
########################
trtRDD = testRDD.map(tokenCheck).reduceByKey(lambda a, b: a + b).filter(lambda e: e[1] > 0)
print("Candidate number={}...".format(trtRDD.count()))
candi_list = []
for t, v in sorted(trtRDD.collect(), key=lambda e: e[1], reverse=True):
    k = t[0].encode('utf-8')    
    final_token_list = []
    ptoken = None 
    for token in k.split():
        dtoken = token.encode('utf-8')
        if dtoken.startswith('*'):
            ctoken = dtoken[1:]
            if isTokenSeen(ctoken):
                v += 0.5
                final_token_list.append("~{}".format(ctoken))
                ptoken = ctoken
            else:
                if (len(ctoken)/3) > 4:
                    sub_token_list = tokenize(dtoken[1:])
                    #print('\tRetokenize {} ({}): \'{}\''.format(ctoken, len(ctoken), ' '.join(sub_token_list)))
                    if sub_token_list:
                        #print('\t{}->{}'.format(token, sub_token_list))
                        v += len(sub_token_list) * 0.5
                        final_token_list.extend(sub_token_list)
                        ptoken = ctoken
                        continue

                # Check collocation suggestion
                #print("Look for collocation...")
                collRDD = look4Coll(ptoken, ctoken)
                for st, v in sorted(collRDD.collect(), key=lambda e: e[1], reverse=True):
                    if t[0] is None:
                        print('\tDo you mean "{}"?'.format(st[1]))
                    else:
                        print('\tDo you mean "{} {}"?'.format(st[0], st[1]))
                final_token_list.append(token)
        else:           
            final_token_list.append(token)
            ptoken = token       

    candi_list.append(((final_token_list, t[1].encode('utf-8')), v))

candi_list = sorted(candi_list, key=lambda e: e[1], reverse=True)
ci = 0
for t, v in candi_list:
    tokenized_rst = " ".join(t[0])
    source_sent = t[1]
    ci += 1
    if ci < topN:
        print("Candidate Tokenized Result={} (score={} by '{}')".format(tokenized_rst, v, source_sent))
    else:
        break
