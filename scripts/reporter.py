from jinja2 import Environment, FileSystemLoader
import os

def generate_html(context):
    # 1. 템플릿 파일이 있는 폴더 설정
    file_loader = FileSystemLoader('templates')
    env = Environment(loader=file_loader)

    # 2. 사용할 템플릿 불러오기
    template = env.get_template('template.html')

    # 3. 데이터(context)를 템플릿에 주입하여 결과물 생성
    output = template.render(context)

    # 4. 결과 HTML 파일로 저장
    output_path = "real_final_report.html"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(output)
    
    return os.path.abspath(output_path)