import download  
import zip

def main():
    download.main(r"SampleData\sample.xlsx")
    print("Download complete!")
    zip.main()
    print("Zipping complete!")
main()