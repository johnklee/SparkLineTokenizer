#!/usr/bin/env python3
#encoding=utf-8
import os
import sys
import jieba

# http://blog.fukuball.com/ru-he-shi-yong-jieba-jie-ba-zhong-wen-fen-ci-cheng-shi/
#sentence = r"獨立音樂需要大家一起來推廣，歡迎加入我們的行列！"
#print("Input: {}".format(sentence))
#words = jieba.cut(sentence, cut_all=False)
#print("Output Full Mode:")
#for word in words:
#    print(word)

if __name__ == '__main__':
    jieba.set_dictionary('cust.dict')
    print("Loading all txt files...")
    for f in os.listdir('.'):
        if f.endswith('.txt'):
            fn = f.split('.')[0]
            dne_fn = "{}.dne".format(fn)
            if os.path.isfile(dne_fn):
                print('\tSkip {}...'.format(f))
                continue

            print('\tHandle {}...'.format(fn))
            with open(dne_fn, 'w') as fw:
                with open(f, 'r') as fh:
                    for line in fh:
                        line = line.strip()
                        if not line or line.startswith('#'):
                            continue

                        words = jieba.cut(line, cut_all=False)
                        fw.write('{}\n'.format(' '.join(words)))
