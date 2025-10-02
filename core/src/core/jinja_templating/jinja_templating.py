import os
import jinja2

class SQLTemplateLoader:
    """
    A class to load and render SQL templates from a directory using Jinja2.
    """
    
    def __init__(self, templates_dir):
        """
        Initialize the SQLTemplateLoader with a directory containing SQL templates.
        
        Args:
            templates_dir (str): Path to the directory containing SQL template files
        """
        self.templates_dir = templates_dir
        self.env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(templates_dir),
            trim_blocks=True,
            lstrip_blocks=True
        )
        self.templates = {}
        self._load_templates()
    
    def _load_templates(self):
        """Load all SQL templates from the templates directory."""
        sql_files = [f for f in os.listdir(self.templates_dir) 
                    if f.endswith('.sql')]
        
        for sql_file in sql_files:
            template_name = os.path.splitext(sql_file)[0]
            self.templates[template_name] = self.env.get_template(sql_file)
            
        print(f"Loaded {len(self.templates)} SQL templates")
    
    def get_template_names(self):
        """Return a list of all template names."""
        return list(self.templates.keys())
    
    def render_template(self, template_name, **kwargs):
        """
        Render a specific template with the provided parameters.
        
        Args:
            template_name (str): Name of the template to render
            **kwargs: Variables to pass to the template
            
        Returns:
            str: The rendered SQL query
            
        Raises:
            KeyError: If the template_name doesn't exist
        """
        if template_name not in self.templates:
            raise KeyError(f"Template '{template_name}' not found")
        
        return self.templates[template_name].render(**kwargs)
    
    def render_all_templates(self, **kwargs):
        """
        Render all templates with the same set of parameters.
        
        Args:
            **kwargs: Variables to pass to all templates
            
        Returns:
            dict: Dictionary mapping template names to rendered queries
        """
        rendered = {}
        for name in self.templates:
            rendered[name] = self.render_template(name, **kwargs)
        
        return rendered