import curses
import npyscreen
import treemap

# This application class serves as a wrapper for the initialization of curses
# and also manages the actual forms of the application

class App(npyscreen.NPSAppManaged):
    def onStart(self):
        self.addForm("MAIN", MainForm, name='TreeMap')
 
# This form class defines the display that will be presented to the user.
class MainForm(npyscreen.FormBaseNew):
    def create(self):
        self.add_handlers({
            "q": self.quit})
        parent = self.parentApp
        parent.database = treemap.get_database(None)
        parent.data = treemap.get_subdir_data(
            parent.database, '/mnt/sit', None, None, None, None,
            None, None, None, None, None, None)
        if parent.data['path'] == '':
            parent.data['path'] = '/'
        self.database_widget = self.add(npyscreen.TitleFixedText,name='database', value=parent.database)
        self.path_widget = self.add(npyscreen.TitleFixedText,name='path', value=parent.data['path'])
        self.size_widget = self.add(
            npyscreen.TitleFixedText,name='size',
            value=treemap.get_bytes_str(parent.data['size']))
        self.num_files_widget = self.add(
            npyscreen.TitleFixedText,name='num_files',
            value=treemap.get_num_str(parent.data['num_files']))
        self.atime_cost_widget = self.add(
            npyscreen.TitleFixedText,name='atime_cost',
            value=treemap.get_num_str(parent.data['atime_cost']))
        self.table_widget = self.add(
            Table,
            values=sorted(parent.data['children'], key=lambda k: k['size'], reverse=True),
            rely=8,
            slow_scroll=True)
        self.editw = 5

    def quit(self, *args, **keywords):
        self.parentApp.switchForm(None)

    def update_header(self):
        self.database_widget.value = self.parentApp.database
        self.database_widget.display()
        self.path_widget.value = self.parentApp.data['path']
        if self.parentApp.data['path'] == '':
            self.parentApp.data['path'] = '/'
        self.path_widget.display()
        self.size_widget.value = treemap.get_bytes_str(self.parentApp.data['size'])
        self.size_widget.display()
        self.num_files_widget.value = treemap.get_num_str(self.parentApp.data['num_files'])
        self.num_files_widget.display()
        self.atime_cost_widget.value = treemap.get_num_str(self.parentApp.data['atime_cost'])
        self.atime_cost_widget.display()

class Table(npyscreen.MultiLineAction):
    def __init__(self, *args, **keywords):
        super(Table, self).__init__(*args, **keywords)
        self.add_handlers({
            curses.KEY_BACKSPACE: self.move_up})

    def display_value(self, vl):
        line_format='{:<20s} {:>10s} {:>10s} {:>10s}'
        return line_format.format(
            vl['name'],
            treemap.get_bytes_str(vl['size']),
            treemap.get_num_str(vl['num_files']),
            treemap.get_num_str(vl['atime_cost']))

    def actionHighlighted(self, vl, key_press):
        app = self.parent.parentApp
        update = False
        if key_press == curses.ascii.LF:
            new_path = app.data['path'] + '/' + vl['name']
            app.data = treemap.get_subdir_data(
                app.database, new_path, None, None, None, None,
                None, None, None, None, None, None)
            update = True
        if update:
            self.values=sorted(app.data['children'], key=lambda k: k['size'], reverse=True)
            self.display()
            self.parent.update_header()

    def move_up(self, *args, **keywords):
        app = self.parent.parentApp
        path_bits = app.data['path'].split('/')
        if len(path_bits) == 3:
            return
        new_path = '/'.join(path_bits[:-1])
        app.data = treemap.get_subdir_data(
            app.database, new_path, None, None, None, None,
            None, None, None, None, None, None)
        self.values=sorted(app.data['children'], key=lambda k: k['size'], reverse=True)
        self.display()
        self.parent.update_header()

if __name__ == '__main__':
    App().run()
