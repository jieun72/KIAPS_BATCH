import os
import pickle
import sys

def convert_data(date) :
    print(">>> convert start. ")
    #공백 추가할 위치 파일 불러오기
    with open('blank_pos.pkl','rb') as f:
        blank_pos = pickle.load(f)

    # 수정할 파일 폴더 설정
    path = 'D:\kiapsdata'
    output_file_prefix='blank_'#출력파일 접두어

    input_file_list=[]
    output_file_list=[]

    for (root, directories, files) in os.walk(path + "/amsua/" + date):
        for file in files:
            if '_InnQC2.txt' in file:            
                input_file_list.append(os.path.join(root,file))
                output_file_list.append(os.path.join(root,output_file_prefix+file))            
            

    for inputfile,outputfile in zip(input_file_list,output_file_list):
    
        with open(inputfile, 'r') as f:
            data = f.readlines()
        
        #공백 삽입
        for i in range(0,len(data)):
            data_i = list(data[i])
            for j in range(0,len(blank_pos)):
                data_i.insert(blank_pos[j],' ')
            data[i]="".join(data_i)
        
        #공백 추가 파일 저장
        with open(outputfile, 'w') as f:
            f.writelines(data)
        print(">>> convert done. ")



if __name__ == '__main__':
    convert_data(sys.argv[1])