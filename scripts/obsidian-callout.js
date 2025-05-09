// scripts/obsidian-callout.js
hexo.extend.filter.register('after_render:html', function (str, data) {
    return str.replace(
      /<blockquote>\s*\[!(\w+)\](.*?)<\/blockquote>/gs,
      (match, type, content) => {
        const titleMatch = content.match(/^(.*?)(<br\s*\/?>)?/);
        const title = titleMatch ? titleMatch[1].trim() : '';
        const body = content.replace(title, '').trim();
  
        return `<blockquote class="callout ${type.toLowerCase()}">
  <strong>${title}</strong><br>${body}
  </blockquote>`;
      }
    );
  });
  