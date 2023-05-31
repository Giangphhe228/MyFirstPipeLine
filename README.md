**Báo cáo project - Phạm Hồng Giang**

Tài liệu mô tả ETL pipeline for shopping website data 








































**MỤC LỤC**

[**I. Giới thiệu về định hướng của pipeline.	3**](#_674vrsgw0f08)**

[1.   Mô tả chung.	3](#_8kvq7yhxs2fk)

[2.  Mô tả chi tiết về các tool sử dụng.	3](#_wqnzy6llt8df)

[3. Workflow.	3](#_em4oe6nue8bj)

[**I. Quá trình thực hiện.	4**](#_vzwdlrwq2g18)

[1.   Định nghĩa.	4](#_ntth8ejwyzqb)

[1.1.   Các thực thể.	4](#_4s8h9uf9xo7j)

[1.2.   Các thuộc tính chính.	4](#_s8drehux1vxt)

[2.  Xây dựng.	5](#_pgos6cb5h8n4)

[2.1.   Mô hình ERD CSDL.	5](#_osxhot8o9uzy)

[2.2.   Crawl dữ liệu.	5](#_jlpmeda5mtc)

[2.3.  ETL data into postgresql.	8](#_mc62eo2yp2o4)

[2.4.   Automated pipeline with airflow.	9](#_xnvoxh5q878)























<a name="_56z1pt4r123u"></a>Phần Nội Dung
# <a name="_674vrsgw0f08"></a>**I. Giới thiệu về định hướng của pipeline.**
## <a name="_8kvq7yhxs2fk"></a>**1.   Mục tiêu.**
`	 `Xây dựng một pipeline để tự động quy trình crawler, extract, transform và load dữ liệu về camera từ 3 trang web thương mại điện tử shopee, tiki, lazada về cơ sở dữ liệu, đồng thời xây dựng mô hình dữ liệu để lưu trữ dữ liệu quan hệ.

## <a name="_wqnzy6llt8df"></a>**2.  Phương pháp.**
Sử dụng các thư viện của python như selenium, request, chromedriver… để crawl dữ liệu các sản phẩm camera và các thông tin liên quan từ 3 trang thương mại điện tử là shopee, lazada, tiki. Sau đó tiếp tục sử dụng pandas và các thư viện thao tác với dữ liệu để biến đổi và làm sạch dữ liệu rồi lưu trữ dưới dạng cơ sở dữ liệu quan hệ với database postgresql. Cuối cùng là tự động hóa luồng ETL trên bằng airflow, crontab để tạo ra một pipeline hoàn chỉnh.
## <a name="_em4oe6nue8bj"></a>**3. Workflow.**


# <a name="_vzwdlrwq2g18"></a>I**I. Quá trình thực hiện.**
## <a name="_ntth8ejwyzqb"></a>**1.   Định nghĩa.**
### <a name="_4s8h9uf9xo7j"></a>	**1.1.   Các thực thể.**
`	`Xây dựng các thực thể cần thiết cho quá trình vận hành của hệ thống bao gồm:

- Brand(Nhãn hàng).
- Comment(Bình luận).
- Product(Sản phẩm).
- Shop(Người bán).
- User(người mua).
### <a name="_s8drehux1vxt"></a>	**1.2.   Các thuộc tính chính.**
- Các sản phẩm(**Product**) sẽ có các thuộc tính cần lưu trữ như sau:
- product id: mã số sản phẩm.
- title: mô tả sản phẩm.
- price: giá sản phẩm.
- discount: phần trăm giảm giá.
- stars review: số các đánh giá.
- location: Địa điểm nơi bán.
- shop id: id của cửa hàng.
- brand id: id của nhãn hàng.


- Các khách hàng(**User**) sẽ có các thuộc tính cần lưu trữ như sau:
- user id: mã số khách hàng.
- username: tên người dùng.


- Các bình luận(**Comment**) sẽ có các thuộc tính cần lưu trữ như sau:
- cmt id: mã số bình luận.
- content: nội dung bình luận.
- create time: thời gian bình luận.
- rating: số sao đánh giá của bình luận cho sản phẩm(1-5 sao).
- like count: số lượt thích bình luận này.
- username: tên khách hàng.(vì một khách hàng có thể đổi tên)
- user id: mã số khách hàng.


- Các cửa hàng(**Shop**) sẽ có các thuộc tính cần lưu trữ như sau:
- shop id: mã số cửa hàng.
- location: địa điểm.
- shop name: tên của cửa hàng.

- Các nhãn hàng(**Brand**) sẽ có các thuộc tính cần lưu trữ như sau:
- brand id: mã số nhãn hàng.
- brand name: tên nhãn hàng.
## <a name="_pgos6cb5h8n4"></a>**2.  Xây dựng.**
### <a name="_osxhot8o9uzy"></a>**2.1.   Mô hình ERD CSDL.**

### <a name="_jlpmeda5mtc"></a>**2.2.   Crawl dữ liệu.**
`		`Trước tiên chúng ta sẽ crawl dữ liệu thô từ các trang thương mại điện tử với các thư viện trong python. 

2\.2.1. Lazada.

`	`Đối với lazada vì không có các APIs hỗ trợ việc lấy dữ liệu chúng ta sẽ lấy dữ liệu từ giao diện với thư viện selenium và sử dụng thêm threading, queue để crawl đa luồng tăng hiệu suất của việc lấy dữ liệu.

`	`Một vài hàm để chạy đa luồng:



Một vài hàm crawl dữ liệu:






2\.2.2. Tiki.

Tiki có các APIs hỗ trợ việc thu thập dữ liệu nên ta sẽ sử dụng thư viện requests để sử dụng các headers, params… để lấy được dữ liệu cần lấy từ các APIs dưới dạng json.

Trước tiên cần setup các giá trị đầu vào cần thiết:

Tạo ra dataframe để lưu trữ dữ liệu json:

Sử dụng phương thức request.get để lấy dữ liệu từ api với các param đã

set up ở trên:

Phương pháp tương tự được áp dụng với việc crawl các comment và thông tin liên quan.

2\.2.2. Shopee.

Tuy shopee có APIs tuy nhiên chúng bị giới hạn quyền truy cập và mã hóa để lấy dữ liệu json nên ta cần sử dụng thư viện request nhưng với phương thức request.url để lấy dữ liệu mã hóa và decode chúng để lấy dữ liệu raw dưới dạng list và chuyển về json.

Phương pháp tương tự được sử dụng để lấy comment và thông tin các shop.

Sau khi crawl dữ liệu sẽ được lưu vào các file excel ở local chuẩn bị cho bước ETL vào CSDL.

### <a name="_mc62eo2yp2o4"></a>**2.3.  ETL data into postgresql.**
`		`Do dữ liệu khi được crawl raw từ các nguồn về sẽ có các tên trường khác nhau, cách sắp xếp khác nhau, hoặc có thể là logic thể hiện khác nhau… nên ta cần trích xuất rồi biến đổi, làm sạch về cùng một tiêu chuẩn để có thể phù hợp cho việc lưu trữ vào cơ sở dữ liệu quan hệ ở đây là postgresql.

Dữ liệu sau khi được xử lý bằng pandas sẽ có các trường như đã mô tả ở phần định nghĩa rồi được load vào CSDL bằng psycopg2 dưới đây là một vài đoạn mô tả:

- Tạo kết nối:



- Tạo các bảng và các quan hệ trong csdl:

- Insert dữ liệu từ các file dữ liệu đã được xử lý vào CSDL(ví dụ về product):



### <a name="_xnvoxh5q878"></a>**2.4.   Automated pipeline with airflow.**
Để tạo một pipeline hoàn chỉnh cho quy trình tự động hóa luồng crawl dữ liệu bên trên ta sẽ sử dụng airflow.

Tạo file Dags và các hàm set up cơ bản:


Sử dụng các operator để gọi đến các function đã định nghĩa trước đó:



⇒ Đến đây ta đã hoàn thành cơ bản pipeline chỉ cần sử dụng airflow để trigger dag này rồi xem kết quả dữ liệu sau khi thu thập và xử lý được lưu trong csdl.




- Display all created table after the run of dag:


\-	Display product table properties:


\-	Display user\_detail table properties:


\-	Display comment table properties:

\-	Display brand table properties:


\-	Display shop table properties:



- Count record:



###

<a name="_jzsggqfondi1"></a>		









