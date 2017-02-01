// Freelancer Theme JavaScript

(function($) {
    "use strict"; // Start of use strict

    // Login Form
    //----------------------------------------------
    // Validation
    $("#navbar-form").validate({
        rules: {
            uid: "required",
            pwd: "required"
        },
        errorClass: "form-invalid"
    });

    // Loading
    //----------------------------------------------
    function remove_loading($form)
    {
        $form.find('[type=submit]').removeClass('error success');
        $form.find('.login-form-main-message').removeClass('show error success').html('');
    }

   // data table
    $(document).ready(function() {
        $('#log').DataTable();
        $('#dl_result').DataTable();
        $('#result').DataTable( {
            dom: 'Bfrtip',
            buttons: [
            'csv'
            ]
        } );
    } );

    


    // jQuery for page scrolling feature - requires jQuery Easing plugin
    $('.page-scroll a').bind('click', function(event) {
        var $anchor = $(this);
        $('html, body').stop().animate({
            scrollTop: ($($anchor.attr('href')).offset().top - 50)
        }, 1250, 'easeInOutExpo');
        event.preventDefault();
    });

    // Highlight the top nav as scrolling occurs
    $('body').scrollspy({
        target: '.navbar-fixed-top',
        offset: 51
    });



    // Floating label headings for the contact form
    $(function() {
        $("body").on("input propertychange", ".floating-label-form-group", function(e) {
            $(this).toggleClass("floating-label-form-group-with-value", !!$(e.target).val());
        }).on("focus", ".floating-label-form-group", function() {
            $(this).addClass("floating-label-form-group-with-focus");
        }).on("blur", ".floating-label-form-group", function() {
            $(this).removeClass("floating-label-form-group-with-focus");
        });
    });

})(jQuery); // End of use strict
