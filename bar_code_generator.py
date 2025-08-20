import pandas as pd
from PIL import Image, ImageDraw, ImageFont
import barcode
from barcode.writer import ImageWriter
from PIL import Image, ImageDraw, ImageFont
import os


def create_equipment_label(equipment_id, description, description2, output_folder, logo_path=None, title="EQUIPMENT LABEL"):
    try:
        code128 = barcode.get_barcode_class('code128')
        barcode_img = code128(equipment_id, writer=ImageWriter())
        
        temp_filename = barcode_img.save("temp_barcode", options={'write_text': False})
        barcode_img = Image.open(temp_filename)
        
        width, height = int(80 * 11.81), int(100 * 11.81)
        label_img = Image.new('RGB', (width, height), 'white')
        draw = ImageDraw.Draw(label_img)
        
        border_margin = 20
        draw.rectangle([border_margin, border_margin, width-border_margin, height-border_margin], 
                      outline="black", width=2)
        
        current_y = border_margin + 80
        
        if logo_path and os.path.exists(logo_path):
            try:
                logo = Image.open(logo_path)
                original_width, original_height = logo.size
                logo_width = int(original_width * 0.5)
                logo_height = int(original_height * 0.5)
                logo = logo.resize((logo_width, logo_height))
                
                logo_position = (border_margin + 10, current_y)
                label_img.paste(logo, logo_position)
                
                try:
                    title_font = ImageFont.truetype("arialbd.ttf", 50) 
                except:
                    title_font = ImageFont.load_default()
                
                title_y = current_y + (logo_height - 36) // 2  
                title_width = draw.textlength(title, font=title_font)
                title_x = max((width - title_width) // 2, logo_width + 30)
                
                draw.text((title_x, title_y), title, font=title_font, fill="black")
                
                current_y += logo_height + 20
            except Exception as e:
                print(f"Error processing logo: {str(e)}")
                try:
                    title_font = ImageFont.truetype("arialbd.ttf", 60)
                except:
                    title_font = ImageFont.load_default()
                
                title_width = draw.textlength(title, font=title_font)
                draw.text(((width - title_width) // 2, current_y), title, font=title_font, fill="black")
                current_y += 40
        else:
            try:
                title_font = ImageFont.truetype("arialbd.ttf", 60)
            except:
                title_font = ImageFont.load_default()
            
            title_width = draw.textlength(title, font=title_font)
            draw.text(((width - title_width) // 2, current_y), title, font=title_font, fill="black")
            current_y += 40
        
        remaining_height = (height - border_margin) - current_y
        text_block_height = 40 + 40 + (40 * len(description.split()))

        barcode_width = min(int(width * 0.80), width - 30)
        barcode_height = int(barcode_width * 0.4)
       
        barcode_y = current_y + ((remaining_height - text_block_height - barcode_height) // 2)
        barcode_x = (width - barcode_width) // 2
        
        barcode_img = barcode_img.resize((barcode_width, barcode_height))
        label_img.paste(barcode_img, (barcode_x, barcode_y))
        
        current_y = barcode_y + barcode_height + 20
        
        try:
            id_font = ImageFont.truetype("arial.ttf", 60)
        except:
            id_font = ImageFont.load_default()
        
        id_text = f"ID: {equipment_id}"
        id_width = draw.textlength(id_text, font=id_font)
        draw.text(((width - id_width) // 2, current_y), id_text, font=id_font, fill="black")
        current_y += 60
        
                # --- Font for description ---
        try:
            if len(description) <= 26:
                desc_font_1 = ImageFont.truetype("arial.ttf", 50)
            else:
                desc_font_1 = ImageFont.truetype("arial.ttf", 33)
        except:
            desc_font_1 = ImageFont.load_default()
        
        try:
            desc_font_2 = ImageFont.truetype("arial.ttf", 50)  # Always size 50 for description2
        except:
            desc_font_2 = ImageFont.load_default()
        
        def wrap_text(text, font, max_width):
            lines = []
            words = text.split()
            while words:
                line = ''
                while words and draw.textlength(line + words[0], font=font) <= max_width:
                    line += (words.pop(0) + ' ')
                lines.append(line.strip())
            return lines
        
        max_text_width = width - 2 * border_margin - 20

        current_y += 20  # Margin after ID

        # Draw second description (description2)
        description2_lines = wrap_text(description2, desc_font_2, max_text_width)
        for line in description2_lines:
            line_width = draw.textlength(line, font=desc_font_2)
            draw.text(((width - line_width) // 2, current_y), line, font=desc_font_2, fill="black")
            current_y += 40

        # Add margin before description (after description2)
        current_y += 15  # Margin between description2 and description

        # Draw first description (description)
        description_lines = wrap_text(description, desc_font_1, max_text_width)
        for line in description_lines:
            line_width = draw.textlength(line, font=desc_font_1)
            draw.text(((width - line_width) // 2, current_y), line, font=desc_font_1, fill="black")
            current_y += 60

       
        # Save output
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        output_path = os.path.join(output_folder, f"{equipment_id}.png")
        label_img.save(output_path)
        print(f"Created label for {equipment_id}")
        
        if os.path.exists(temp_filename):
            os.remove(temp_filename)
            
    except Exception as e:
        print(f"Error creating label for {equipment_id}: {str(e)}")


def main():
 
    excel_file = fr"C:\\Users\egoba.kelvin\\automation scripts\\qrcode.xlsx"  
    output_folder = fr"C:\\Users\egoba.kelvin\\automation scripts\\barcode"
    logo = fr"C:\\Users\egoba.kelvin\\automation scripts\\logo.png"
    title="KELLOGG NOODLES ESWATINI"
    
    try:
        #
        df = pd.read_excel(excel_file)
        
        
        if 'Equipment' not in df.columns or 'Description' not in df.columns:
            print("Error: Excel file must contain 'Equipment' and 'Description' columns")
            return
        
      
        for index, row in df.iterrows():
            equipment_id = str(row['Equipment'])
            description = str(row['Description'])
            functional = str(row["Functional Loc."])
            create_equipment_label(equipment_id, description,functional, output_folder, logo, title)
            
        print(f"\nSuccessfully created {len(df)} labels in '{output_folder}' folder")
    
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()